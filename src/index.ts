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