#!/usr/bin/env python3
"""
Trino ODBC Driver for Alpine Linux
Enables .NET applications to connect to Trino via ODBC
"""

import os
import sys
import json
import logging
import argparse
from typing import Dict, List, Optional, Any, Union, Tuple
import uuid
import socket
import threading
import queue
import time
from datetime import datetime

# Third-party dependencies
import pyodbc
import trino
from trino.auth import BasicAuthentication
from trino.dbapi import Connection as TrinoConnection
from trino.dbapi import Cursor as TrinoCursor
from flask import Flask, request, jsonify, Response

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/trino-odbc-driver.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("trino-odbc-driver")

# Constants
VERSION = "1.0.0"
DEFAULT_PORT = 8991
DRIVER_NAME = "Trino ODBC Driver for Alpine Linux"

class TrinoODBCError(Exception):
    """Base exception class for Trino ODBC driver errors"""
    pass

class ConnectionManager:
    """Manages connections to Trino servers"""
    
    def __init__(self):
        self.connections: Dict[str, TrinoConnection] = {}
        self.cursors: Dict[str, TrinoCursor] = {}
        self.connection_params: Dict[str, Dict] = {}
        self.lock = threading.Lock()
    
    def create_connection(self, params: Dict[str, Any]) -> str:
        """
        Create a new Trino connection with the given parameters
        
        Args:
            params: Connection parameters including host, port, user, etc.
            
        Returns:
            A unique connection ID
        """
        connection_id = str(uuid.uuid4())
        
        try:
            auth = None
            if params.get('user') and params.get('password'):
                auth = BasicAuthentication(params['user'], params['password'])
            
            conn = trino.dbapi.connect(
                host=params.get('host', 'localhost'),
                port=params.get('port', 8080),
                user=params.get('user', 'trino'),
                catalog=params.get('catalog'),
                schema=params.get('schema'),
                auth=auth,
                http_scheme=params.get('http_scheme', 'http'),
                verify=params.get('verify', True),
                session_properties=params.get('session_properties', {})
            )
            
            with self.lock:
                self.connections[connection_id] = conn
                self.connection_params[connection_id] = params
                
            logger.info(f"Created connection {connection_id} to {params.get('host')}:{params.get('port')}")
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            raise TrinoODBCError(f"Failed to connect to Trino: {str(e)}")
    
    def close_connection(self, connection_id: str) -> bool:
        """
        Close a Trino connection
        
        Args:
            connection_id: The connection ID to close
            
        Returns:
            True if the connection was closed successfully
        """
        with self.lock:
            if connection_id in self.connections:
                # Close any open cursors for this connection
                cursors_to_close = [cursor_id for cursor_id, cursor in self.cursors.items() 
                                   if cursor_id.startswith(connection_id)]
                
                for cursor_id in cursors_to_close:
                    self.cursors[cursor_id].close()
                    del self.cursors[cursor_id]
                
                # Close the connection
                self.connections[connection_id].close()
                del self.connections[connection_id]
                del self.connection_params[connection_id]
                
                logger.info(f"Closed connection {connection_id}")
                return True
            else:
                logger.warning(f"Attempted to close non-existent connection {connection_id}")
                return False
    
    def create_cursor(self, connection_id: str) -> str:
        """
        Create a new cursor for an existing connection
        
        Args:
            connection_id: The connection ID to create a cursor for
            
        Returns:
            A unique cursor ID
        """
        with self.lock:
            if connection_id not in self.connections:
                raise TrinoODBCError(f"Connection {connection_id} does not exist")
            
            cursor_id = f"{connection_id}_{str(uuid.uuid4())}"
            self.cursors[cursor_id] = self.connections[connection_id].cursor()
            
            logger.info(f"Created cursor {cursor_id} for connection {connection_id}")
            return cursor_id
    
    def close_cursor(self, cursor_id: str) -> bool:
        """
        Close a cursor
        
        Args:
            cursor_id: The cursor ID to close
            
        Returns:
            True if the cursor was closed successfully
        """
        with self.lock:
            if cursor_id in self.cursors:
                self.cursors[cursor_id].close()
                del self.cursors[cursor_id]
                
                logger.info(f"Closed cursor {cursor_id}")
                return True
            else:
                logger.warning(f"Attempted to close non-existent cursor {cursor_id}")
                return False
    
    def execute_query(self, cursor_id: str, query: str, parameters: Optional[List] = None) -> Dict:
        """
        Execute a SQL query using the specified cursor
        
        Args:
            cursor_id: The cursor ID to use
            query: The SQL query to execute
            parameters: Optional parameters for the query
            
        Returns:
            Dictionary with execution status and column information
        """
        with self.lock:
            if cursor_id not in self.cursors:
                raise TrinoODBCError(f"Cursor {cursor_id} does not exist")
        
        try:
            cursor = self.cursors[cursor_id]
            
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            # Get column information if available
            columns = []
            if cursor.description:
                columns = [
                    {
                        "name": col[0],
                        "type_code": col[1],
                        "display_size": col[2],
                        "internal_size": col[3],
                        "precision": col[4],
                        "scale": col[5],
                        "null_ok": col[6]
                    }
                    for col in cursor.description
                ]
            
            logger.info(f"Executed query on cursor {cursor_id}: {query[:100]}...")
            return {
                "success": True,
                "columns": columns,
                "rowcount": cursor.rowcount if cursor.rowcount >= 0 else -1
            }
            
        except Exception as e:
            logger.error(f"Query execution error on cursor {cursor_id}: {str(e)}")
            raise TrinoODBCError(f"Query execution error: {str(e)}")
    
    def fetch_results(self, cursor_id: str, max_rows: int = 1000) -> Dict:
        """
        Fetch results from a previously executed query
        
        Args:
            cursor_id: The cursor ID to fetch results from
            max_rows: Maximum number of rows to fetch at once
            
        Returns:
            Dictionary containing the fetched rows and status
        """
        with self.lock:
            if cursor_id not in self.cursors:
                raise TrinoODBCError(f"Cursor {cursor_id} does not exist")
        
        try:
            cursor = self.cursors[cursor_id]
            rows = cursor.fetchmany(max_rows)
            
            # Convert rows to a list of dictionaries if possible
            result_rows = []
            if cursor.description:
                column_names = [col[0] for col in cursor.description]
                for row in rows:
                    result_rows.append(dict(zip(column_names, row)))
            else:
                result_rows = [list(row) for row in rows]
            
            has_more = len(rows) >= max_rows
            
            logger.info(f"Fetched {len(rows)} rows from cursor {cursor_id}")
            return {
                "success": True,
                "rows": result_rows,
                "has_more": has_more
            }
            
        except Exception as e:
            logger.error(f"Error fetching results from cursor {cursor_id}: {str(e)}")
            raise TrinoODBCError(f"Error fetching results: {str(e)}")
    
    def get_connection_info(self, connection_id: str) -> Dict:
        """
        Get information about a connection
        
        Args:
            connection_id: The connection ID to get information for
            
        Returns:
            Dictionary containing connection information
        """
        with self.lock:
            if connection_id not in self.connections:
                raise TrinoODBCError(f"Connection {connection_id} does not exist")
            
            params = self.connection_params[connection_id]
            
            # Return a copy of the connection parameters without sensitive info
            info = params.copy()
            if 'password' in info:
                info['password'] = '***'
            
            return info

# Flask REST API for the ODBC driver
app = Flask(__name__)
connection_manager = ConnectionManager()

@app.route('/status', methods=['GET'])
def status():
    """Health check endpoint for the driver"""
    return jsonify({
        "status": "ok",
        "version": VERSION,
        "name": DRIVER_NAME
    })

@app.route('/connections', methods=['POST'])
def create_connection():
    """Create a new connection to Trino"""
    try:
        params = request.json
        connection_id = connection_manager.create_connection(params)
        return jsonify({
            "success": True,
            "connection_id": connection_id
        })
    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/connections/<connection_id>', methods=['DELETE'])
def close_connection(connection_id):
    """Close a connection to Trino"""
    try:
        success = connection_manager.close_connection(connection_id)
        return jsonify({
            "success": success
        })
    except Exception as e:
        logger.error(f"Error closing connection {connection_id}: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/connections/<connection_id>/cursors', methods=['POST'])
def create_cursor(connection_id):
    """Create a new cursor for a connection"""
    try:
        cursor_id = connection_manager.create_cursor(connection_id)
        return jsonify({
            "success": True,
            "cursor_id": cursor_id
        })
    except Exception as e:
        logger.error(f"Error creating cursor for connection {connection_id}: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/cursors/<cursor_id>', methods=['DELETE'])
def close_cursor(cursor_id):
    """Close a cursor"""
    try:
        success = connection_manager.close_cursor(cursor_id)
        return jsonify({
            "success": success
        })
    except Exception as e:
        logger.error(f"Error closing cursor {cursor_id}: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/cursors/<cursor_id>/execute', methods=['POST'])
def execute_query(cursor_id):
    """Execute a SQL query using a cursor"""
    try:
        data = request.json
        query = data.get('query')
        parameters = data.get('parameters')
        
        if not query:
            return jsonify({
                "success": False,
                "error": "Query is required"
            }), 400
            
        result = connection_manager.execute_query(cursor_id, query, parameters)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error executing query on cursor {cursor_id}: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/cursors/<cursor_id>/fetch', methods=['GET'])
def fetch_results(cursor_id):
    """Fetch results from a cursor"""
    try:
        max_rows = request.args.get('max_rows', 1000, type=int)
        result = connection_manager.fetch_results(cursor_id, max_rows)
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error fetching results from cursor {cursor_id}: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/connections/<connection_id>/info', methods=['GET'])
def get_connection_info(connection_id):
    """Get information about a connection"""
    try:
        info = connection_manager.get_connection_info(connection_id)
        return jsonify({
            "success": True,
            "info": info
        })
    except Exception as e:
        logger.error(f"Error getting info for connection {connection_id}: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

def main():
    """Main entry point for the ODBC driver service"""
    parser = argparse.ArgumentParser(description=f"{DRIVER_NAME} v{VERSION}")
    parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                        help=f'Port to run the service on (default: {DEFAULT_PORT})')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='Host to bind the service to (default: 0.0.0.0)')
    parser.add_argument('--debug', action='store_true',
                        help='Run in debug mode')
    
    args = parser.parse_args()
    
    logger.info(f"Starting {DRIVER_NAME} v{VERSION} on {args.host}:{args.port}")
    
    # Run the Flask app
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()
