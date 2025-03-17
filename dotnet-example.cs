using System;
using System.Data;
using TrinoODBC;

namespace TrinoODBCExample
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Trino ODBC Driver .NET Example Application");
            Console.WriteLine("===========================================");
            
            // Connection parameters - replace with your own values
            string trinoHost = "localhost";
            int trinoPort = 8080;
            string trinoCatalog = "hive";
            string trinoSchema = "default";
            string trinoUser = "trino";
            
            // ODBC Driver service parameters
            string driverHost = "localhost";
            int driverPort = 8991;
            
            // Build connection string
            string connectionString = $"server={driverHost};port={driverPort};" + 
                                     $"host={trinoHost};port={trinoPort};" + 
                                     $"user={trinoUser};catalog={trinoCatalog};schema={trinoSchema}";
            
            Console.WriteLine($"Connecting to Trino at {trinoHost}:{trinoPort} using catalog '{trinoCatalog}'...");
            
            using (var connection = new TrinoConnection(connectionString))
            {
                try
                {
                    // Open connection
                    connection.Open();
                    Console.WriteLine("Connection established successfully!");
                    
                    // Example 1: Simple SELECT query
                    SimpleQueryExample(connection);
                    
                    // Example 2: Parameterized query
                    ParameterizedQueryExample(connection);
                    
                    // Example 3: Metadata query
                    MetadataQueryExample(connection);
                    
                    // Example 4: INSERT example (commented out for safety)
                    // InsertExample(connection);
                    
                    // Example 5: Execute Scalar query
                    ScalarQueryExample(connection);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
                    }
                }
                finally
                {
                    // Ensure connection is closed
                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        Console.WriteLine("Connection closed.");
                    }
                }
            }
            
            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
        
        static void SimpleQueryExample(TrinoConnection connection)
        {
            Console.WriteLine("\n--- Simple Query Example ---");
            
            using (var command = connection.CreateCommand())
            {
                // Replace 'your_table' with an actual table name in your Trino catalog/schema
                command.CommandText = "SELECT * FROM your_table LIMIT 5";
                
                Console.WriteLine($"Executing query: {command.CommandText}");
                
                using (var reader = command.ExecuteReader())
                {
                    // Print column names
                    Console.WriteLine("\nResults:");
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Console.Write($"{reader.GetName(i),-15}");
                    }
                    Console.WriteLine();
                    
                    // Print separator line
                    Console.WriteLine(new string('-', reader.FieldCount * 15));
                    
                    // Print rows
                    int rowCount = 0;
                    while (reader.Read())
                    {
                        rowCount++;
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write($"{reader.GetValue(i),-15}");
                        }
                        Console.WriteLine();
                    }
                    
                    Console.WriteLine($"\nTotal rows: {rowCount}");
                }
            }
        }
        
        static void ParameterizedQueryExample(TrinoConnection connection)
        {
            Console.WriteLine("\n--- Parameterized Query Example ---");
            
            using (var command = connection.CreateCommand())
            {
                // Replace 'your_table' and 'your_column' with actual names in your Trino database
                command.CommandText = "SELECT * FROM your_table WHERE your_column = ? LIMIT 3";
                
                // Add parameter - replace 'parameter_value' with an actual value
                var parameter = command.CreateParameter();
                parameter.ParameterName = "p1"; // Trino uses positional parameters
                parameter.Value = "parameter_value";
                command.Parameters.Add(parameter);
                
                Console.WriteLine($"Executing parameterized query with value: {parameter.Value}");
                
                using (var reader = command.ExecuteReader())
                {
                    // Print column names
                    Console.WriteLine("\nResults:");
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        Console.Write($"{reader.GetName(i),-15}");
                    }
                    Console.WriteLine();
                    
                    // Print separator line
                    Console.WriteLine(new string('-', reader.FieldCount * 15));
                    
                    // Print rows
                    int rowCount = 0;
                    while (reader.Read())
                    {
                        rowCount++;
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write($"{reader.GetValue(i),-15}");
                        }
                        Console.WriteLine();
                    }
                    
                    Console.WriteLine($"\nTotal rows: {rowCount}");
                }
            }
        }
        
        static void MetadataQueryExample(TrinoConnection connection)
        {
            Console.WriteLine("\n--- Metadata Query Example ---");
            
            // Get catalog and schema from connection
            string catalog = connection.ConnectionString.Split(';')
                .FirstOrDefault(s => s.StartsWith("catalog="))?.Substring(8) ?? "hive";
            
            string schema = connection.ConnectionString.Split(';')
                .FirstOrDefault(s => s.StartsWith("schema="))?.Substring(7) ?? "default";
            
            using (var command = connection.CreateCommand())
            {
                // Query to get tables in the current schema
                command.CommandText = $"SHOW TABLES FROM {catalog}.{schema}";
                
                Console.WriteLine($"Getting tables from {catalog}.{schema}...");
                
                using (var reader = command.ExecuteReader())
                {
                    Console.WriteLine("\nTables:");
                    int tableCount = 0;
                    
                    while (reader.Read())
                    {
                        tableCount++;
                        string tableName = reader.GetString(0);
                        Console.WriteLine($"  {tableCount}. {tableName}");
                    }
                    
                    if (tableCount == 0)
                    {
                        Console.WriteLine("  No tables found.");
                    }
                    else
                    {
                        Console.WriteLine($"\nTotal tables: {tableCount}");
                    }
                }
            }
        }
        
        static void InsertExample(TrinoConnection connection)
        {
            Console.WriteLine("\n--- Insert Example ---");
            
            using (var command = connection.CreateCommand())
            {
                // Replace with a valid table in your Trino database
                command.CommandText = "INSERT INTO your_table (column1, column2) VALUES (?, ?)";
                
                // Add parameters
                var param1 = command.CreateParameter();
                param1.ParameterName = "p1";
                param1.Value = "Value 1";
                command.Parameters.Add(param1);
                
                var param2 = command.CreateParameter();
                param2.ParameterName = "p2";
                param2.Value = "Value 2";
                command.Parameters.Add(param2);
                
                Console.WriteLine($"Executing insert with values: {param1.Value}, {param2.Value}");
                
                int rowsAffected = command.ExecuteNonQuery();
                Console.WriteLine($"Rows affected: {rowsAffected}");
            }
        }
        
        static void ScalarQueryExample(TrinoConnection connection)
        {
            Console.WriteLine("\n--- Scalar Query Example ---");
            
            using (var command = connection.CreateCommand())
            {
                // Replace 'your_table' with an actual table name in your Trino database
                command.CommandText = "SELECT COUNT(*) FROM your_table";
                
                Console.WriteLine($"Executing query: {command.CommandText}");
                
                object result = command.ExecuteScalar();
                Console.WriteLine($"Result: {result}");
            }
        }
    }
}
