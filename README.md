# ClickHouse MCP Server

A Model Context Protocol (MCP) server that connects to ClickHouse databases and allows LLMs like Claude to explore and analyze data through natural language queries.

## Features

- Connect to ClickHouse databases
- Expose table schemas as resources
- Run SQL queries from natural language instructions
- Execute read-only SQL queries
- Works with Claude Desktop for macOS

## Installation

1. Clone this repository
2. Install dependencies:

```bash
npm install
```

3. Configure your environment variables in a `.env` file (see `.env.example` for reference). **Ensure you are using a read-only database user to restrict DDL and DML execution**
4. Build the project:

```bash
npm run build
```

## Usage

### Running the server locally

```bash
npm start
```

### Integrating with Claude Desktop

1. Create or update your Claude Desktop configuration file:
   - On macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

2. Add the server configuration:

```json
{
  "mcpServers": {
    "clickhouse-analytics": {
      "command": "node",
      "args": [
        "/absolute/path/to/clickhouse-mcp-server/dist/index.js"
      ],
      "env": {
        "CLICKHOUSE_URL": "your_clickhouse_url",
        "CLICKHOUSE_USERNAME": "your_username",
        "CLICKHOUSE_PASSWORD": "your_password",
        "CLICKHOUSE_DATABASE": "your_database"
      }
    }
  }
}
```

3. Restart Claude Desktop

### Available Features

#### Resources

- `db://info` - Database information including tables and schemas
- `table://{tableName}/schema` - Schema for a specific table
- `table://{tableName}/sample` - Sample data (5 rows) from a specific table

#### Tools

- `execute-sql` - Run a read-only SQL query against the database
- `natural-language-query` - Ask questions about your data in natural language


## Security Considerations

- This server only allows read-only SQL queries
- Sensitive credentials should be stored securely in environment variables
- The server performs basic validation to prevent DDL or DML statement execution
- **Always review query requests before execution**

## License

MIT