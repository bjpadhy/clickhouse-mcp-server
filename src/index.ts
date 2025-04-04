import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import ClickHouseService from "./clickhouse-service.js";

// Create a new ClickHouse service instance
const clickhouseService = new ClickHouseService();

// Create an MCP server
const server = new McpServer({
  name: "clickhouse-mcp-server",
  version: "1.0.0",
  capabilities: {
    resources: {},
    tools: {},
    prompts: {}
  }
});

// Register resources - Database Schema
server.resource(
  "database-info",
  "db://info",
  async (uri) => {
    try {
      const dbInfo = await clickhouseService.getDatabaseInfo();
      return {
        contents: [{
          uri: uri.href,
          text: JSON.stringify(dbInfo, null, 2),
          mimeType: "application/json"
        }]
      };
    } catch (error) {
      console.error("Error fetching database info:", error);
      throw error;
    }
  }
);

// Register resources - Table Schema
server.resource(
  "table-schema",
  new ResourceTemplate("table://{tableName}/schema", { list: async () => {
    const tables = await clickhouseService.listTables();
    return {
      resources: tables.map(table => ({
        name: `Schema for ${table}`,
        uri: `table://${table}/schema`
      }))
    };
  }}),
  async (uri, { tableName }) => {
    try {
      // Make sure tableName is a string
      const tableNameStr = Array.isArray(tableName) ? tableName[0] : tableName;
      const schema = await clickhouseService.getTableSchema(tableNameStr);
      return {
        contents: [{
          uri: uri.href,
          text: JSON.stringify(schema, null, 2),
          mimeType: "application/json"
        }]
      };
    } catch (error) {
      console.error(`Error fetching schema for table ${tableName}:`, error);
      throw error;
    }
  }
);

// Register resources - Table sample data
server.resource(
  "table-sample",
  new ResourceTemplate("table://{tableName}/sample", { list: async () => {
    const tables = await clickhouseService.listTables();
    return {
      resources: tables.map(table => ({
        name: `Sample data for ${table}`,
        uri: `table://${table}/sample`
      }))
    };
  }}),
  async (uri, { tableName }) => {
    try {
      // Make sure tableName is a string
      const tableNameStr = Array.isArray(tableName) ? tableName[0] : tableName;
      const sampleData = await clickhouseService.getTableSampleData(tableNameStr);
      return {
        contents: [{
          uri: uri.href,
          text: JSON.stringify(sampleData, null, 2),
          mimeType: "application/json"
        }]
      };
    } catch (error) {
      console.error(`Error fetching sample data for table ${tableName}:`, error);
      throw error;
    }
  }
);

// Register tool - Running SQL queries
server.tool(
  "execute-sql",
  {
    query: z.string().describe("The SQL query to execute (read-only operations only)")
  },
  async ({ query }) => {
    try {
      // Execute the query
      const results = await clickhouseService.executeQuery(query);
      
      // Format the results nicely
      let formattedResults = JSON.stringify(results, null, 2);
      
      return {
        content: [{ type: "text", text: formattedResults }]
      };
    } catch (error) {
      console.error("Error executing SQL query:", error);
      const errorMessage = error instanceof Error ? error.message : String(error);
      return {
        content: [{ type: "text", text: `Error: ${errorMessage}` }],
        isError: true
      };
    }
  }
);

// Register tool - natural language query
server.tool(
  "natural-language-query",
  {
    question: z.string().describe("The natural language question about your data")
  },
  async ({ question }) => {
    try {
      // First get database schema to provide context
      const dbInfo = await clickhouseService.getDatabaseInfo();
      
      // Form a response with context
      const schemaInfo = Object.entries(dbInfo.schemas)
        .map(([tableName, columns]) => {
          return `Table "${tableName}" has columns: ${(columns as any[]).map(col => `${col.name} (${col.type})`).join(', ')}`;
        })
        .join('\n\n');
      
      // Return both the database schema and the question so the LLM can generate a suitable SQL query
      return {
        content: [{ 
          type: "text", 
          text: `Based on your question: "${question}", here is the database schema to reference:\n\n${schemaInfo}\n\nTo answer this question, you could use a SQL query like: \n\nSELECT * FROM ... WHERE ... LIMIT 10;\n\nYou can execute specific SQL queries using the "execute-sql" tool.` 
        }]
      };
    } catch (error) {
      console.error("Error processing natural language query:", error);
      const errorMessage = error instanceof Error ? error.message : String(error);
      return {
        content: [{ type: "text", text: `Error: ${errorMessage}` }],
        isError: true
      };
    }
  }
);

// // Register prompts for common data analysis tasks
// server.prompt(
//   "data-summary",
//   { tableName: z.string() },
//   ({ tableName }) => ({
//     messages: [{
//       role: "user",
//       content: {
//         type: "text",
//         text: `Please provide a summary of the data in the "${tableName}" table. First, get the schema using the table-schema resource. Then, get sample data using the table-sample resource. Finally, use execute-sql to get counts and basic statistics.`
//       }
//     }]
//   })
// );

// server.prompt(
//   "trend-analysis",
//   { 
//     tableName: z.string(),
//     dateColumn: z.string(),
//     metricColumn: z.string()
//   },
//   ({ tableName, dateColumn, metricColumn }) => ({
//     messages: [{
//       role: "user",
//       content: {
//         type: "text",
//         text: `Please analyze trends in the "${metricColumn}" column of the "${tableName}" table over time, using the "${dateColumn}" column as the time dimension. 
        
// 1. First, check the schema of the table to confirm the column types.
// 2. Then, look at sample data to understand the format.
// 3. Generate and execute SQL queries to analyze:
//    - Overall trend over time
//    - Any seasonality or patterns
//    - Notable outliers or anomalies
// 4. Provide visualizations if possible.`
//       }
//     }]
//   })
// );

// server.prompt(
//   "correlation-analysis",
//   { 
//     tableName: z.string(),
//     column1: z.string(),
//     column2: z.string()
//   },
//   ({ tableName, column1, column2 }) => ({
//     messages: [{
//       role: "user",
//       content: {
//         type: "text",
//         text: `Please analyze the correlation between the "${column1}" and "${column2}" columns in the "${tableName}" table.
        
// 1. First, check the schema of the table to confirm the column types.
// 2. Then, look at sample data to understand the data.
// 3. Generate and execute SQL queries to calculate:
//    - Correlation statistics
//    - Groupings or patterns
//    - Any interesting insights about how these variables relate
// 4. Provide a summary of your findings.`
//       }
//     }]
//   })
// );

// Start the server
async function startServer() {
  try {
    console.error("Starting ClickHouse MCP Server...");
    
    // Create a transport for stdio communication
    const transport = new StdioServerTransport();
    
    // Connect the server to the transport
    await server.connect(transport);
    
    console.error("ClickHouse MCP Server running on stdio");
    
    // Handle shutdown
    process.on('SIGINT', async () => {
      console.error("Shutting down ClickHouse MCP Server...");
      await clickhouseService.close();
      process.exit(0);
    });
  } catch (error) {
    console.error("Error starting server:", error);
    await clickhouseService.close();
    process.exit(1);
  }
}

startServer();