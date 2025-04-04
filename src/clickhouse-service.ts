import { createClient, ClickHouseClient } from '@clickhouse/client';
import 'dotenv/config';

// Environment variables
const CLICKHOUSE_URL = process.env.CLICKHOUSE_URL as string;
const CLICKHOUSE_USERNAME = process.env.CLICKHOUSE_USERNAME as string;
const CLICKHOUSE_PASSWORD = process.env.CLICKHOUSE_PASSWORD as string;
const CLICKHOUSE_DATABASE = process.env.CLICKHOUSE_DATABASE as string;

// Validate environment variables
if (!CLICKHOUSE_URL || !CLICKHOUSE_USERNAME || !CLICKHOUSE_PASSWORD || !CLICKHOUSE_DATABASE) {
  throw new Error('Missing required ClickHouse environment variables');
}

interface TableRow {
  name: string;
}

interface ColumnInfo {
  name: string;
  type: string;
}

class ClickHouseService {
  private client: ClickHouseClient;

  constructor() {
    this.client = createClient({
      host: CLICKHOUSE_URL,
      username: CLICKHOUSE_USERNAME,
      password: CLICKHOUSE_PASSWORD,
      database: CLICKHOUSE_DATABASE
    });
  }

  /**
   * Get a list of all tables in the database
   * @returns Promise resolving to array of table names
   */
  async listTables(): Promise<string[]> {
    try {
      const result = await this.client.query({
        query: `SHOW TABLES FROM ${CLICKHOUSE_DATABASE}`,
        format: 'JSONEachRow',
      });
      
      const data = await result.json<TableRow[]>();
      return data.map((table) => table.name);
    } catch (error: unknown) {
      console.error('Error listing tables:', error);
      throw error;
    }
  }

  /**
   * Get the schema for a specific table
   * @param tableName - Name of the table to get schema for
   * @returns Promise resolving to array of column information
   */
  async getTableSchema(tableName: string): Promise<ColumnInfo[]> {
    try {
      const result = await this.client.query({
        query: `DESCRIBE TABLE ${CLICKHOUSE_DATABASE}.${tableName}`,
        format: 'JSONEachRow',
      });
      
      const data = await result.json<ColumnInfo[]>();
      return data;
    } catch (error: unknown) {
      console.error(`Error getting schema for table ${tableName}:`, error);
      throw error;
    }
  }

  /**
   * Execute a read-only SQL query
   * @param query - SQL query to execute
   * @returns Promise resolving to query results
   * @throws Error if non-read-only query is attempted
   */
  async executeQuery<T = Record<string, unknown>>(query: string): Promise<T[]> {
    try {
      // Basic validation to ensure only read-only queries are executed; any missed validation should be handled by the database read-only user
      const normalizedQuery = query.trim().toLowerCase();
      if (
        normalizedQuery.includes('insert') ||
        normalizedQuery.includes('update') ||
        normalizedQuery.includes('delete') ||
        normalizedQuery.includes('drop') ||
        normalizedQuery.includes('alter') ||
        normalizedQuery.includes('create')
      ) {
        throw new Error('Only read-only queries are allowed');
      }

      const result = await this.client.query({
        query,
        format: 'JSONEachRow',
      });
      
      const data = await result.json<T[]>();
      return data;
    } catch (error: unknown) {
      console.error('Error executing query:', error);
      throw error;
    }
  }

  /**
   * Get table and schema information for the entire database
   * @returns Promise resolving to database information
   */
  async getDatabaseInfo(): Promise<{
    database: string;
    tables: string[];
    schemas: Record<string, ColumnInfo[]>;
  }> {
    try {
      const tables = await this.listTables();
      const tableSchemas: Record<string, ColumnInfo[]> = {};
      
      for (const table of tables) {
        tableSchemas[table] = await this.getTableSchema(table);
      }
      
      return {
        database: CLICKHOUSE_DATABASE,
        tables,
        schemas: tableSchemas
      };
    } catch (error: unknown) {
      console.error('Error getting database info:', error);
      throw error;
    }
  }

  /**
   * Get sample data from a table (limited to 5 rows)
   * @param tableName - Name of the table to get sample data from
   * @returns Promise resolving to sample data rows
   */
  async getTableSampleData<T = Record<string, unknown>>(tableName: string): Promise<T[]> {
    try {
      const result = await this.client.query({
        query: `SELECT * FROM ${CLICKHOUSE_DATABASE}.${tableName} LIMIT 5`,
        format: 'JSONEachRow',
      });
      
      const data = await result.json<T[]>();
      return data;
    } catch (error: unknown) {
      console.error(`Error getting sample data for table ${tableName}:`, error);
      throw error;
    }
  }

  /**
   * Close the ClickHouse client connection
   */
  async close(): Promise<void> {
    await this.client.close();
  }
}

export default ClickHouseService;