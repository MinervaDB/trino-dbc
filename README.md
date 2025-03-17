# Trino ODBC Driver for Alpine Linux

This driver provides a bridge between .NET applications and Trino databases for Alpine Linux environments.

## Overview

The solution consists of two main components:

1. **Python-based ODBC Driver Service**: A Flask-based REST API that communicates with Trino and exposes ODBC-like functionality.
2. **.NET Connector**: A .NET library that implements the standard DbConnection, DbCommand, and related classes to communicate with the Python service.

## Installation

### Building the Docker Image

1. Create a directory for the project and place all files into it:
   - `trino_odbc_driver.py` (The main Python service)
   - `Dockerfile`
   - `requirements.txt`

2. Build the Docker image:
   ```bash
   docker build -t trino-odbc-driver .
   ```

3. Run the container:
   ```bash
   docker run -d -p 8991:8991 --name trino-odbc trino-odbc-driver
   ```

### Installing the .NET Connector

1. Create a new .NET project or open your existing project
2. Add the `TrinoODBC.cs` file to your project
3. Make sure to reference the System.Data namespace

## Usage

### .NET Application Example

```csharp
using System;
using System.Data;
using TrinoODBC;

class Program
{
    static void Main(string[] args)
    {
        // Connection string format
        string connectionString = "server=localhost;port=8991;host=trino-server;port=8080;user=trino;catalog=hive;schema=default";
        
        using (var connection = new TrinoConnection(connectionString))
        {
            try
            {
                connection.Open();
                Console.WriteLine("Connected to Trino!");
                
                // Create a command
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT * FROM your_table LIMIT 10";
                    
                    // Execute the command and get results
                    using (var reader = command.ExecuteReader())
                    {
                        // Print column names
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write(reader.GetName(i) + "\t");
                        }
                        Console.WriteLine();
                        
                        // Print rows
                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write(reader.GetValue(i) + "\t");
                            }
                            Console.WriteLine();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }
    }
}
```

### Connection String Format

The connection string consists of two parts:
1. Parameters for connecting to the ODBC driver service:
   - `server`: The hostname or IP of the machine running the ODBC driver service
   - `port`: The port of the ODBC driver service (default: 8991)

2. Parameters for connecting to Trino:
   - `host`: The hostname or IP of the Trino server
   - `port`: The port of the Trino server (default: 8080)
   - `user`: The username for connecting to Trino
   - `password`: (Optional) The password for connecting to Trino
   - `catalog`: The catalog to use
   - `schema`: (Optional) The schema to use
   - `http_scheme`: (Optional) The HTTP scheme to use (default: http)
   - `verify`: (Optional) Whether to verify SSL certificates (default: true)

## Advanced Usage

### Parameterized Queries

```csharp
using (var command = connection.CreateCommand())
{
    command.CommandText = "SELECT * FROM your_table WHERE id = @id";
    
    // Add parameter
    var parameter = command.CreateParameter();
    parameter.ParameterName = "@id";
    parameter.Value = 123;
    command.Parameters.Add(parameter);
    
    // Execute command
    using (var reader = command.ExecuteReader())
    {
        // Process results...
    }
}
```

### Executing Non-Query Commands

```csharp
using (var command = connection.CreateCommand())
{
    command.CommandText = "INSERT INTO your_table (column1, column2) VALUES (@value1, @value2)";
    
    // Add parameters
    command.Parameters.AddWithValue("@value1", "Hello");
    command.Parameters.AddWithValue("@value2", "World");
    
    // Execute non-query command
    int rowsAffected = command.ExecuteNonQuery();
    Console.WriteLine($"Rows affected: {rowsAffected}");
}
```

### Executing Scalar Queries

```csharp
using (var command = connection.CreateCommand())
{
    command.CommandText = "SELECT COUNT(*) FROM your_table";
    
    // Execute scalar query
    object result = command.ExecuteScalar();
    Console.WriteLine($"Count: {result}");
}
```

## Troubleshooting

### Driver Service Logs

The Python ODBC driver service logs to `/var/log/trino-odbc-driver.log` inside the container. To view these logs:

```bash
docker exec -it trino-odbc cat /var/log/trino-odbc-driver.log
```

### Testing the REST API

You can test the REST API directly:

```bash
# Check driver status
curl http://localhost:8991/status

# Create a connection
curl -X POST -H "Content-Type: application/json" \
  -d '{"host":"trino-server","port":8080,"user":"trino","catalog":"hive"}' \
  http://localhost:8991/connections
```

## Limitations

- The current implementation does not support transactions
- Only the Text command type is supported
- Only Input parameter direction is supported
- Large objects (LOBs) are not fully supported

## Performance Considerations

- The driver fetches results in batches of 1000 rows to optimize memory usage
- For large result sets, consider adding LIMIT clauses to your queries
- Connection pooling is recommended for applications that make frequent connections

## Security Considerations

- The driver service does not include built-in authentication
- Consider running the service behind a reverse proxy with authentication
- For production use, enable HTTPS by configuring a reverse proxy with SSL/TLS
