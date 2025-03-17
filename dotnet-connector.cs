using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace TrinoODBC
{
    /// <summary>
    /// Trino ODBC Driver Connection for .NET applications
    /// </summary>
    public class TrinoConnection : DbConnection
    {
        private readonly HttpClient _httpClient;
        private string _connectionString;
        private ConnectionState _state;
        private string _connectionId;
        private string _serverUrl;
        private Dictionary<string, string> _connectionParams;

        /// <summary>
        /// Creates a new Trino connection
        /// </summary>
        public TrinoConnection()
        {
            _httpClient = new HttpClient();
            _state = ConnectionState.Closed;
            _connectionParams = new Dictionary<string, string>();
        }

        /// <summary>
        /// Creates a new Trino connection with the specified connection string
        /// </summary>
        /// <param name="connectionString">The connection string</param>
        public TrinoConnection(string connectionString) : this()
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Gets or sets the connection string
        /// </summary>
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (_state != ConnectionState.Closed)
                {
                    throw new InvalidOperationException("Cannot change connection string while the connection is open");
                }

                _connectionString = value;
                ParseConnectionString(value);
            }
        }

        /// <summary>
        /// Gets the current database
        /// </summary>
        public override string Database => _connectionParams.ContainsKey("schema") ? _connectionParams["schema"] : string.Empty;

        /// <summary>
        /// Gets the data source
        /// </summary>
        public override string DataSource => _connectionParams.ContainsKey("host") ? _connectionParams["host"] : string.Empty;

        /// <summary>
        /// Gets the server version
        /// </summary>
        public override string ServerVersion => "Trino ODBC Driver";

        /// <summary>
        /// Gets the connection state
        /// </summary>
        public override ConnectionState State => _state;

        /// <summary>
        /// Gets the connection timeout
        /// </summary>
        public override int ConnectionTimeout => _connectionParams.ContainsKey("timeout") 
            ? int.Parse(_connectionParams["timeout"]) 
            : 30;

        /// <summary>
        /// Begins a database transaction
        /// </summary>
        /// <returns>A transaction object</returns>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("Transactions are not supported by the Trino ODBC driver");
        }

        /// <summary>
        /// Changes the current database
        /// </summary>
        /// <param name="databaseName">The name of the database to use</param>
        public override void ChangeDatabase(string databaseName)
        {
            if (_state != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            _connectionParams["schema"] = databaseName;
        }

        /// <summary>
        /// Closes the connection
        /// </summary>
        public override void Close()
        {
            if (_state == ConnectionState.Open)
            {
                try
                {
                    if (!string.IsNullOrEmpty(_connectionId))
                    {
                        var response = _httpClient.DeleteAsync($"{_serverUrl}/connections/{_connectionId}").Result;
                        if (response.IsSuccessStatusCode)
                        {
                            _connectionId = null;
                            _state = ConnectionState.Closed;
                        }
                        else
                        {
                            throw new DbException($"Failed to close connection: {response.ReasonPhrase}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new DbException($"Error closing connection: {ex.Message}", ex);
                }
            }
        }

        /// <summary>
        /// Opens the connection
        /// </summary>
        public override void Open()
        {
            if (_state == ConnectionState.Open)
            {
                return;
            }

            if (string.IsNullOrEmpty(_serverUrl))
            {
                throw new InvalidOperationException("Server URL is not set in the connection string");
            }

            try
            {
                var parameters = new Dictionary<string, object>();

                // Copy connection parameters
                foreach (var param in _connectionParams)
                {
                    if (param.Key != "driver" && param.Key != "port" && param.Key != "server")
                    {
                        parameters[param.Key] = param.Value;
                    }
                }

                var content = new StringContent(
                    JsonSerializer.Serialize(parameters),
                    Encoding.UTF8,
                    "application/json");

                var response = _httpClient.PostAsync($"{_serverUrl}/connections", content).Result;
                
                if (response.IsSuccessStatusCode)
                {
                    var jsonResponse = JsonDocument.Parse(response.Content.ReadAsStringAsync().Result);
                    var success = jsonResponse.RootElement.GetProperty("success").GetBoolean();
                    
                    if (success)
                    {
                        _connectionId = jsonResponse.RootElement.GetProperty("connection_id").GetString();
                        _state = ConnectionState.Open;
                    }
                    else
                    {
                        var error = jsonResponse.RootElement.GetProperty("error").GetString();
                        throw new DbException($"Failed to open connection: {error}");
                    }
                }
                else
                {
                    throw new DbException($"Failed to open connection: {response.ReasonPhrase}");
                }
            }
            catch (Exception ex)
            {
                throw new DbException($"Error opening connection: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Opens the connection asynchronously
        /// </summary>
        /// <returns>A task representing the asynchronous operation</returns>
        public override async Task OpenAsync(System.Threading.CancellationToken cancellationToken)
        {
            if (_state == ConnectionState.Open)
            {
                return;
            }

            if (string.IsNullOrEmpty(_serverUrl))
            {
                throw new InvalidOperationException("Server URL is not set in the connection string");
            }

            try
            {
                var parameters = new Dictionary<string, object>();

                // Copy connection parameters
                foreach (var param in _connectionParams)
                {
                    if (param.Key != "driver" && param.Key != "port" && param.Key != "server")
                    {
                        parameters[param.Key] = param.Value;
                    }
                }

                var content = new StringContent(
                    JsonSerializer.Serialize(parameters),
                    Encoding.UTF8,
                    "application/json");

                var response = await _httpClient.PostAsync($"{_serverUrl}/connections", content, cancellationToken);
                
                if (response.IsSuccessStatusCode)
                {
                    var jsonResponse = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
                    var success = jsonResponse.RootElement.GetProperty("success").GetBoolean();
                    
                    if (success)
                    {
                        _connectionId = jsonResponse.RootElement.GetProperty("connection_id").GetString();
                        _state = ConnectionState.Open;
                    }
                    else
                    {
                        var error = jsonResponse.RootElement.GetProperty("error").GetString();
                        throw new DbException($"Failed to open connection: {error}");
                    }
                }
                else
                {
                    throw new DbException($"Failed to open connection: {response.ReasonPhrase}");
                }
            }
            catch (Exception ex)
            {
                throw new DbException($"Error opening connection: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Creates a command
        /// </summary>
        /// <returns>A command object</returns>
        protected override DbCommand CreateDbCommand()
        {
            return new TrinoCommand(this, _httpClient, _serverUrl);
        }

        /// <summary>
        /// Parse the connection string into individual parameters
        /// </summary>
        /// <param name="connectionString">The connection string to parse</param>
        private void ParseConnectionString(string connectionString)
        {
            _connectionParams.Clear();

            // Parse connection string in format "key1=value1;key2=value2"
            if (!string.IsNullOrEmpty(connectionString))
            {
                foreach (var part in connectionString.Split(';'))
                {
                    if (!string.IsNullOrEmpty(part))
                    {
                        var keyValue = part.Trim().Split('=', 2);
                        if (keyValue.Length == 2)
                        {
                            var key = keyValue[0].ToLower().Trim();
                            var value = keyValue[1].Trim();
                            _connectionParams[key] = value;
                        }
                    }
                }
            }

            // Set the server URL
            if (_connectionParams.ContainsKey("server") && _connectionParams.ContainsKey("port"))
            {
                _serverUrl = $"http://{_connectionParams["server"]}:{_connectionParams["port"]}";
            }
        }
    }

    /// <summary>
    /// Trino ODBC Driver Command for .NET applications
    /// </summary>
    public class TrinoCommand : DbCommand
    {
        private readonly TrinoConnection _connection;
        private readonly HttpClient _httpClient;
        private readonly string _serverUrl;
        private string _commandText;
        private CommandType _commandType;
        private int _commandTimeout;
        private bool _designTimeVisible;
        private UpdateRowSource _updateRowSource;
        private List<TrinoParameter> _parameters;
        private string _cursorId;

        /// <summary>
        /// Creates a new Trino command
        /// </summary>
        /// <param name="connection">The connection</param>
        /// <param name="httpClient">The HTTP client</param>
        /// <param name="serverUrl">The server URL</param>
        internal TrinoCommand(TrinoConnection connection, HttpClient httpClient, string serverUrl)
        {
            _connection = connection;
            _httpClient = httpClient;
            _serverUrl = serverUrl;
            _parameters = new List<TrinoParameter>();
            _commandTimeout = 30;
            _commandType = CommandType.Text;
            _updateRowSource = UpdateRowSource.None;
            _designTimeVisible = true;
        }

        /// <summary>
        /// Gets or sets the connection
        /// </summary>
        protected override DbConnection DbConnection
        {
            get => _connection;
            set
            {
                if (!(value is TrinoConnection))
                {
                    throw new InvalidOperationException("Connection must be a TrinoConnection");
                }

                if (_connection != value)
                {
                    DisposeCurrentCursor();
                }
            }
        }

        /// <summary>
        /// Gets or sets the transaction
        /// </summary>
        protected override DbTransaction DbTransaction
        {
            get => null;
            set => throw new NotSupportedException("Transactions are not supported by the Trino ODBC driver");
        }

        /// <summary>
        /// Gets or sets the command text
        /// </summary>
        public override string CommandText
        {
            get => _commandText;
            set
            {
                if (_commandText != value)
                {
                    DisposeCurrentCursor();
                    _commandText = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the command timeout
        /// </summary>
        public override int CommandTimeout
        {
            get => _commandTimeout;
            set => _commandTimeout = value;
        }

        /// <summary>
        /// Gets or sets the command type
        /// </summary>
        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new NotSupportedException("Only Text command type is supported by the Trino ODBC driver");
                }
                _commandType = value;
            }
        }

        /// <summary>
        /// Gets or sets whether the command should be visible at design time
        /// </summary>
        public override bool DesignTimeVisible
        {
            get => _designTimeVisible;
            set => _designTimeVisible = value;
        }

        /// <summary>
        /// Gets or sets how command results are applied to the DataRow
        /// </summary>
        public override UpdateRowSource UpdatedRowSource
        {
            get => _updateRowSource;
            set => _updateRowSource = value;
        }

        /// <summary>
        /// Gets the collection of parameters
        /// </summary>
        protected override DbParameterCollection DbParameterCollection => new TrinoParameterCollection(_parameters);

        /// <summary>
        /// Cancels the command
        /// </summary>
        public override void Cancel()
        {
            throw new NotSupportedException("Cancel is not supported by the Trino ODBC driver");
        }

        /// <summary>
        /// Creates a parameter
        /// </summary>
        /// <returns>A parameter object</returns>
        protected override DbParameter CreateDbParameter()
        {
            return new TrinoParameter();
        }

        /// <summary>
        /// Executes a SQL statement and returns the number of rows affected
        /// </summary>
        /// <returns>The number of rows affected</returns>
        public override int ExecuteNonQuery()
        {
            using (var reader = ExecuteDbDataReader(CommandBehavior.Default))
            {
                return reader.RecordsAffected;
            }
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row
        /// </summary>
        /// <returns>The first column of the first row</returns>
        public override object ExecuteScalar()
        {
            using (var reader = ExecuteDbDataReader(CommandBehavior.SingleRow))
            {
                if (reader.Read())
                {
                    return reader.GetValue(0);
                }
                return null;
            }
        }

        /// <summary>
        /// Executes the command and returns a data reader
        /// </summary>
        /// <param name="behavior">The command behavior</param>
        /// <returns>A data reader</returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_connection == null)
            {
                throw new InvalidOperationException("Connection is not set");
            }

            if (_connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            if (string.IsNullOrEmpty(_commandText))
            {
                throw new InvalidOperationException("Command text is not set");
            }

            try
            {
                // Create a cursor
                var connectionId = (_connection as TrinoConnection)._connectionId;
                var cursorResponse = _httpClient.PostAsync(
                    $"{_serverUrl}/connections/{connectionId}/cursors",
                    null).Result;

                if (!cursorResponse.IsSuccessStatusCode)
                {
                    throw new DbException($"Failed to create cursor: {cursorResponse.ReasonPhrase}");
                }

                var cursorJson = JsonDocument.Parse(cursorResponse.Content.ReadAsStringAsync().Result);
                var cursorSuccess = cursorJson.RootElement.GetProperty("success").GetBoolean();

                if (!cursorSuccess)
                {
                    var error = cursorJson.RootElement.GetProperty("error").GetString();
                    throw new DbException($"Failed to create cursor: {error}");
                }

                _cursorId = cursorJson.RootElement.GetProperty("cursor_id").GetString();

                // Execute the query
                var parameters = new Dictionary<string, object>
                {
                    ["query"] = _commandText
                };

                // Add parameters if any
                if (_parameters.Count > 0)
                {
                    var paramValues = new List<object>();
                    foreach (TrinoParameter param in _parameters)
                    {
                        paramValues.Add(param.Value);
                    }
                    parameters["parameters"] = paramValues;
                }

                var executeContent = new StringContent(
                    JsonSerializer.Serialize(parameters),
                    Encoding.UTF8,
                    "application/json");

                var executeResponse = _httpClient.PostAsync(
                    $"{_serverUrl}/cursors/{_cursorId}/execute",
                    executeContent).Result;

                if (!executeResponse.IsSuccessStatusCode)
                {
                    throw new DbException($"Failed to execute query: {executeResponse.ReasonPhrase}");
                }

                var executeJson = JsonDocument.Parse(executeResponse.Content.ReadAsStringAsync().Result);
                var executeSuccess = executeJson.RootElement.GetProperty("success").GetBoolean();

                if (!executeSuccess)
                {
                    var error = executeJson.RootElement.GetProperty("error").GetString();
                    throw new DbException($"Failed to execute query: {error}");
                }

                // Return a data reader
                return new TrinoDataReader(_httpClient, _serverUrl, _cursorId, behavior);
            }
            catch (Exception ex)
            {
                throw new DbException($"Error executing command: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Prepares the command
        /// </summary>
        public override void Prepare()
        {
            // Trino doesn't support prepared statements in the same way as other databases
            // This method is a no-op
        }

        /// <summary>
        /// Disposes the current cursor if one exists
        /// </summary>
        private void DisposeCurrentCursor()
        {
            if (!string.IsNullOrEmpty(_cursorId))
            {
                try
                {
                    _httpClient.DeleteAsync($"{_serverUrl}/cursors/{_cursorId}").Wait();
                }
                catch
                {
                    // Ignore errors when disposing
                }
                finally
                {
                    _cursorId = null;
                }
            }
        }

        /// <summary>
        /// Disposes the command
        /// </summary>
        /// <param name="disposing">Whether to dispose managed resources</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposeCurrentCursor();
            }
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Trino ODBC Driver Parameter for .NET applications
    /// </summary>
    public class TrinoParameter : DbParameter
    {
        private string _parameterName;
        private object _value;
        private ParameterDirection _direction;
        private bool _isNullable;
        private int _size;
        private string _sourceColumn;
        private bool _sourceColumnNullMapping;
        private DataRowVersion _sourceVersion;

        /// <summary>
        /// Creates a new Trino parameter
        /// </summary>
        public TrinoParameter()
        {
            _direction = ParameterDirection.Input;
            _sourceVersion = DataRowVersion.Current;
        }

        /// <summary>
        /// Gets or sets the parameter name
        /// </summary>
        public override string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value;
        }

        /// <summary>
        /// Gets or sets the parameter value
        /// </summary>
        public override object Value
        {
            get => _value;
            set => _value = value;
        }

        /// <summary>
        /// Gets or sets the parameter direction
        /// </summary>
        public override ParameterDirection Direction
        {
            get => _direction;
            set
            {
                if (value != ParameterDirection.Input)
                {
                    throw new NotSupportedException("Only Input parameters are supported by the Trino ODBC driver");
                }
                _direction = value;
            }
        }

        /// <summary>
        /// Gets or sets whether the parameter accepts null values
        /// </summary>
        public override bool IsNullable
        {
            get => _isNullable;
            set => _isNullable = value;
        }

        /// <summary>
        /// Gets or sets the parameter DbType
        /// </summary>
        public override DbType DbType { get; set; }

        /// <summary>
        /// Gets or sets the size of the parameter
        /// </summary>
        public override int Size
        {
            get => _size;
            set => _size = value;
        }

        /// <summary>
        /// Gets or sets the source column
        /// </summary>
        public override string SourceColumn
        {
            get => _sourceColumn;
            set => _sourceColumn = value;
        }

        /// <summary>
        /// Gets or sets the source column null mapping
        /// </summary>
        public override bool SourceColumnNullMapping
        {
            get => _sourceColumnNullMapping;
            set => _sourceColumnNullMapping = value;
        }

        /// <summary>
        /// Gets or sets the source version
        /// </summary>
        public override DataRowVersion SourceVersion
        {
            get => _sourceVersion;
            set => _sourceVersion = value;
        }

        /// <summary>
        /// Resets the parameter
        /// </summary>
        public override void ResetDbType()
        {
            DbType = DbType.String;
        }
    }

    /// <summary>
    /// Trino ODBC Driver Parameter Collection for .NET applications
    /// </summary>
    public class TrinoParameterCollection : DbParameterCollection
    {
        private readonly List<TrinoParameter> _parameters;

        /// <summary>
        /// Creates a new Trino parameter collection
        /// </summary>
        /// <param name="parameters">The list of parameters</param>
        public TrinoParameterCollection(List<TrinoParameter> parameters)
        {
            _parameters = parameters;
        }

        /// <summary>
        /// Gets the number of parameters in the collection
        /// </summary>
        public override int Count => _parameters.Count;

        /// <summary>
        /// Gets whether the collection has a fixed size
        /// </summary>
        public override bool IsFixedSize => false;

        /// <summary>
        /// Gets whether the collection is read-only
        /// </summary>
        public override bool IsReadOnly => false;

        /// <summary>
        /// Gets whether the collection is synchronized
        /// </summary>
        public override bool IsSynchronized => false;

        /// <summary>
        /// Gets the synchronization root object
        /// </summary>
        public override object SyncRoot => _parameters;

        /// <summary>
        /// Gets or sets a parameter at the specified index
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>The parameter</returns>
        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        /// <summary>
        /// Gets a parameter by name
        /// </summary>
        /// <param name="parameterName">The parameter name</param>
        /// <returns>The parameter</returns>
        protected override DbParameter GetParameter(string parameterName)
        {
            return _parameters.Find(p => p.ParameterName == parameterName);
        }

        /// <summary>
        /// Sets a parameter at the specified index
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">The parameter</param>
        protected override void SetParameter(int index, DbParameter value)
        {
            if (!(value is TrinoParameter))
            {
                throw new InvalidOperationException("Parameter must be a TrinoParameter");
            }

            _parameters[index] = (TrinoParameter)value;
        }

        /// <summary>
        /// Sets a parameter by name
        /// </summary>
        /// <param name="parameterName">The parameter name</param>
        /// <param name="value">The parameter</param>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            if (!(value is TrinoParameter))
            {
                throw new InvalidOperationException("Parameter must be a TrinoParameter");
            }

            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                _parameters[index] = (TrinoParameter)value;
            }
            else
            {
                throw new IndexOutOfRangeException($"Parameter {parameterName} not found");
            }
        }

        /// <summary>
        /// Adds a parameter to the collection
        /// </summary>
        /// <param name="value">The parameter</param>
        /// <returns>The index of the added parameter</returns>
        public override int Add(object value)
        {
            if (!(value is TrinoParameter))
            {
                throw new InvalidOperationException("Parameter must be a TrinoParameter");
            }

            _parameters.Add((TrinoParameter)value);
            return _parameters.Count - 1;
        }

        /// <summary>
        /// Adds a parameter with the specified name and value
        /// </summary>
        /// <param name="parameterName">The parameter name</param>
        /// <param name="value">The parameter value</param>
        /// <returns>The added parameter</returns>
        public override DbParameter AddWithValue(string parameterName, object value)
        {
            var parameter = new TrinoParameter
            {
                ParameterName = parameterName,
                Value = value
            };

            Add(parameter);
            return parameter;
        }

        /// <summary>
        /// Clears the collection
        /// </summary>
        public override void Clear()
        {
            _parameters.Clear();
        }

        /// <summary>
        /// Checks if the collection contains the specified parameter
        /// </summary>
        /// <param name="value">The parameter</param>
        /// <returns>True if the collection contains the parameter</returns>
        public override bool Contains(object value)
        {
            if (!(value is TrinoParameter))
            {
                return false;
            }

            return _parameters.Contains((TrinoParameter)value);
        }

        /// <summary>
        /// Checks if the collection contains a parameter with the specified name
        /// </summary>
        /// <param name="value">The parameter name</param>
        /// <returns>True if the collection contains the parameter</returns>
        public override bool Contains(string value)
        {
            return _parameters.Exists(p => p.ParameterName == value);
        }

        /// <summary>
        /// Copies the parameters to an array
        /// </summary>
        /// <param name="array">The destination array</param>
        /// <param name="arrayIndex">The starting index in the destination array</param>
        public override void CopyTo(Array array, int arrayIndex)
        {
            for (int i = 0; i < _parameters.Count; i++)
            {
                array.SetValue(_parameters[i], arrayIndex + i);
            }
        }

        /// <summary>
        /// Gets an enumerator for the collection
        /// </summary>
        /// <returns>An enumerator</returns>
        public override System.Collections.IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        /// <summary>
        /// Gets the index of a parameter
        /// </summary>
        /// <param name="value">The parameter</param>
        /// <returns>The index of the parameter</returns>
        public override int IndexOf(object value)
        {
            if (!(value is TrinoParameter))
            {
                return -1;
            }

            return _parameters.IndexOf((TrinoParameter)value);
        }

        /// <summary>
        /// Gets the index of a parameter by name
        /// </summary>
        /// <param name="parameterName">The parameter name</param>
        /// <returns>The index of the parameter</returns>
        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < _parameters.Count; i++)
            {
                if (_parameters[i].ParameterName == parameterName)
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Inserts a parameter at the specified index
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">The parameter</param>
        public override void Insert(int index, object value)
        {
            if (!(value is TrinoParameter))
            {
                throw new InvalidOperationException("Parameter must be a TrinoParameter");
            }

            _parameters.Insert(index, (TrinoParameter)value);
        }

        /// <summary>
        /// Removes a parameter from the collection
        /// </summary>
        /// <param name="value">The parameter</param>
        public override void Remove(object value)
        {
            if (value is TrinoParameter)
            {
                _parameters.Remove((TrinoParameter)value);
            }
        }

        /// <summary>
        /// Removes a parameter at the specified index
        /// </summary>
        /// <param name="index">The index</param>
        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        /// <summary>
        /// Removes a parameter by name
        /// </summary>
        /// <param name="parameterName">The parameter name</param>
        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
            {
                _parameters.RemoveAt(index);
            }
        }
    }

    /// <summary>
    /// Trino ODBC Driver Data Reader for .NET applications
    /// </summary>
    public class TrinoDataReader : DbDataReader
    {
        private readonly HttpClient _httpClient;
        private readonly string _serverUrl;
        private readonly string _cursorId;
        private readonly CommandBehavior _behavior;
        private List<Dictionary<string, object>> _currentBatch;
        private int _currentRowIndex;
        private bool _hasMoreRows;
        private bool _isClosed;
        private List<string> _columnNames;
        private Dictionary<string, int> _columnNameToIndex;
        private int _recordsAffected;

        /// <summary>
        /// Creates a new Trino data reader
        /// </summary>
        /// <param name="httpClient">The HTTP client</param>
        /// <param name="serverUrl">The server URL</param>
        /// <param name="cursorId">The cursor ID</param>
        /// <param name="behavior">The command behavior</param>
        public TrinoDataReader(HttpClient httpClient, string serverUrl, string cursorId, CommandBehavior behavior)
        {
            _httpClient = httpClient;
            _serverUrl = serverUrl;
            _cursorId = cursorId;
            _behavior = behavior;
            _currentBatch = new List<Dictionary<string, object>>();
            _currentRowIndex = -1;
            _hasMoreRows = true;
            _isClosed = false;
            _columnNames = new List<string>();
            _columnNameToIndex = new Dictionary<string, int>();
            _recordsAffected = -1;

            // Fetch the first batch
            FetchNextBatch();
        }

        /// <summary>
        /// Gets the number of columns in the current row
        /// </summary>
        public override int FieldCount => _columnNames.Count;

        /// <summary>
        /// Gets a value indicating whether the data reader is closed
        /// </summary>
        public override bool IsClosed => _isClosed;

        /// <summary>
        /// Gets the number of rows affected by the command
        /// </summary>
        public override int RecordsAffected => _recordsAffected;

        /// <summary>
        /// Gets a value indicating whether the data reader has rows
        /// </summary>
        public override bool HasRows => _currentBatch.Count > 0 || _hasMoreRows;

        /// <summary>
        /// Gets the depth of nesting for the current row
        /// </summary>
        public override int Depth => 0;

        /// <summary>
        /// Gets the value of the specified column
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal</param>
        /// <returns>The value</returns>
        public override object this[int ordinal] => GetValue(ordinal);

        /// <summary>
        /// Gets the value of the specified column
        /// </summary>
        /// <param name="name">The name of the column</param>
        /// <returns>The value</returns>
        public override object this[string name] => GetValue(GetOrdinal(name));

        /// <summary>
        /// Closes the data reader
        /// </summary>
        public override void Close()
        {
            if (!_isClosed)
            {
                try
                {
                    _httpClient.DeleteAsync($"{_serverUrl}/cursors/{_cursorId}").Wait();
                }
                catch
                {
                    // Ignore errors when closing
                }
                finally
                {
                    _isClosed = true;
                }
            }
        }

        /// <summary>
        /// Gets the name of the specified column
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal</param>
        /// <returns>The name of the column</returns>
        public override string GetName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= _columnNames.Count)
            {
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");
            }

            return _columnNames[ordinal];
        }

        /// <summary>
        /// Gets the data type of the specified column
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal</param>
        /// <returns>The data type</returns>
        public override Type GetFieldType(int ordinal)
        {
            if (_currentRowIndex < 0 || _currentRowIndex >= _currentBatch.Count)
            {
                throw new InvalidOperationException("No current row");
            }

            if (ordinal < 0 || ordinal >= _columnNames.Count)
            {
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");
            }

            var columnName = _columnNames[ordinal];
            var value = _currentBatch[_currentRowIndex][columnName];

            return value?.GetType() ?? typeof(object);
        }

        /// <summary>
        /// Gets the data type name of the specified column
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal</param>
        /// <returns>The data type name</returns>
        public override string GetDataTypeName(int ordinal)
        {
            return GetFieldType(ordinal).Name;
        }

        /// <summary>
        /// Gets the value of the specified column
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal</param>
        /// <returns>The value</returns>
        public override object GetValue(int ordinal)
        {
            if (_currentRowIndex < 0 || _currentRowIndex >= _currentBatch.Count)
            {
                throw new InvalidOperationException("No current row");
            }

            if (ordinal < 0 || ordinal >= _columnNames.Count)
            {
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");
            }

            var columnName = _columnNames[ordinal];
            return _currentBatch[_currentRowIndex][columnName] ?? DBNull.Value;
        }

        /// <summary>
        /// Gets the values of all columns in the current row
        /// </summary>
        /// <param name="values">An array of objects to copy the values into</param>
        /// <returns>The number of values copied</returns>
        public override int GetValues(object[] values)
        {
            if (_currentRowIndex < 0 || _currentRowIndex >= _currentBatch.Count)
            {
                throw new InvalidOperationException("No current row");
            }

            var count = Math.Min(values.Length, _columnNames.Count);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }

            return count;
        }

        /// <summary>
        /// Gets the ordinal of the specified column
        /// </summary>
        /// <param name="name">The name of the column</param>
        /// <returns>The zero-based column ordinal</returns>
        public override int GetOrdinal(string name)
        {
            if (_columnNameToIndex.TryGetValue(name, out var ordinal))
            {
                return ordinal;
            }

            throw new IndexOutOfRangeException($"Column {name} not found");
        }

        /// <summary>
        /// Gets a value indicating whether the specified column is null
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal</param>
        /// <returns>True if the column is null</returns>
        public override bool IsDBNull(int ordinal)
        {
            return GetValue(ordinal) == DBNull.Value;
        }

        /// <summary>
        /// Advances the data reader to the next record
        /// </summary>
        /// <returns>True if there are more rows</returns>
        public override bool Read()
        {
            if (_isClosed)
            {
                throw new InvalidOperationException("Data reader is closed");
            }

            if (_behavior == CommandBehavior.SchemaOnly)
            {
                return false;
            }

            _currentRowIndex++;

            if (_currentRowIndex >= _currentBatch.Count)
            {
                if (_hasMoreRows)
                {
                    // Fetch the next batch
                    FetchNextBatch();
                    _currentRowIndex = 0;
                }
                else
                {
                    return false;
                }
            }

            return _currentRowIndex < _currentBatch.Count;
        }

        /// <summary>
        /// Advances the data reader to the next result set
        /// </summary>
        /// <returns>True if there are more result sets</returns>
        public override bool NextResult()
        {
            // Trino doesn't support multiple result sets
            return false;
        }

        /// <summary>
        /// Fetches the next batch of records
        /// </summary>
        private void FetchNextBatch()
        {
            try
            {
                var max_rows = 1000;
                var response = _httpClient.GetAsync($"{_serverUrl}/cursors/{_cursorId}/fetch?max_rows={max_rows}").Result;

                if (!response.IsSuccessStatusCode)
                {
                    throw new DbException($"Failed to fetch results: {response.ReasonPhrase}");
                }

                var jsonResponse = JsonDocument.Parse(response.Content.ReadAsStringAsync().Result);
                var success = jsonResponse.RootElement.GetProperty("success").GetBoolean();

                if (!success)
                {
                    var error = jsonResponse.RootElement.GetProperty("error").GetString();
                    throw new DbException($"Failed to fetch results: {error}");
                }

                _hasMoreRows = jsonResponse.RootElement.GetProperty("has_more").GetBoolean();
                
                // Get the rows
                var rows = new List<Dictionary<string, object>>();
                var rowsElement = jsonResponse.RootElement.GetProperty("rows");
                
                foreach (var rowElement in rowsElement.EnumerateArray())
                {
                    var row = new Dictionary<string, object>();
                    foreach (var property in rowElement.EnumerateObject())
                    {
                        if (property.Value.ValueKind == JsonValueKind.Null)
                        {
                            row[property.Name] = null;
                        }
                        else if (property.Value.ValueKind == JsonValueKind.String)
                        {
                            row[property.Name] = property.Value.GetString();
                        }
                        else if (property.Value.ValueKind == JsonValueKind.Number)
                        {
                            if (property.Value.TryGetInt64(out var intValue))
                            {
                                row[property.Name] = intValue;
                            }
                            else
                            {
                                row[property.Name] = property.Value.GetDouble();
                            }
                        }
                        else if (property.Value.ValueKind == JsonValueKind.True || property.Value.ValueKind == JsonValueKind.False)
                        {
                            row[property.Name] = property.Value.GetBoolean();
                        }
                        else
                        {
                            row[property.Name] = property.Value.GetRawText();
                        }
                    }
                    rows.Add(row);
                }

                _currentBatch = rows;

                // Initialize column names if this is the first batch
                if (_columnNames.Count == 0 && _currentBatch.Count > 0)
                {
                    _columnNames = new List<string>(_currentBatch[0].Keys);
                    for (int i = 0; i < _columnNames.Count; i++)
                    {
                        _columnNameToIndex[_columnNames[i]] = i;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new DbException($"Error fetching results: {ex.Message}", ex);
            }
        }

        // Additional method implementations for various GetXXX methods

        public override bool GetBoolean(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToBoolean(value);
        }

        public override byte GetByte(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToByte(value);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException("GetBytes is not supported by the Trino ODBC driver");
        }

        public override char GetChar(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToChar(value);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException("GetChars is not supported by the Trino ODBC driver");
        }

        public override Guid GetGuid(int ordinal)
        {
            var value = GetValue(ordinal);
            if (value is string str)
            {
                return Guid.Parse(str);
            }
            return (Guid)value;
        }

        public override short GetInt16(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToInt16(value);
        }

        public override int GetInt32(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToInt32(value);
        }

        public override long GetInt64(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToInt64(value);
        }

        public override float GetFloat(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToSingle(value);
        }

        public override double GetDouble(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToDouble(value);
        }

        public override string GetString(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToString(value);
        }

        public override decimal GetDecimal(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToDecimal(value);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToDateTime(value);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_isClosed)
            {
                Close();
            }
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Custom exception for database errors
    /// </summary>
    public class DbException : Exception
    {
        public DbException(string message) : base(message)
        {
        }

        public DbException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
