#!/usr/bin/env python3
"""Unit tests for DynamoDB Local setup functionality."""

from unittest.mock import patch, MagicMock
import subprocess
import pytest

from awslabs.dynamodb_mcp_server.dynamodb_local_setup import (
    is_docker_available,
    find_available_port,
    get_running_container_endpoint,
    start_docker_container,
    setup_dynamodb_local
)


class TestDynamoDBLocalSetup:
    """Test cases for DynamoDB Local setup functionality."""
    
    def test_is_docker_available_success(self):
        """Test Docker availability check when Docker is available."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.shutil.which') as mock_which, \
             patch('subprocess.run') as mock_run:
            mock_which.return_value = "/usr/local/bin/docker"
            mock_run.return_value = MagicMock()
            
            result = is_docker_available()
            assert result is True
            
            mock_which.assert_called_once_with("docker")
            mock_run.assert_called_once_with(
                ["/usr/local/bin/docker", "--version"], 
                capture_output=True, 
                check=True, 
                timeout=5
            )

    def test_is_docker_available_failure(self):
        """Test Docker availability check when Docker is not available."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.shutil.which') as mock_which:
            mock_which.return_value = None  # Docker not found
            result = is_docker_available()
            assert result is False

    def test_find_first_available_port(self):
        """Test finding available port when first port is free."""
        with patch('socket.socket') as mock_socket:
            mock_sock = MagicMock()
            mock_sock.connect_ex.return_value = 1  # Connection failed = port free
            mock_socket.return_value.__enter__.return_value = mock_sock
            
            port = find_available_port(8000)
            assert port == 8000
            mock_sock.connect_ex.assert_called_once_with(('localhost', 8000))

    def test_find_next_available_port(self):
        """Test finding available port when first port is busy."""
        with patch('socket.socket') as mock_socket:
            mock_sock = MagicMock()
            # First call returns 0 (busy), second returns 1 (free)
            mock_sock.connect_ex.side_effect = [0, 1]
            mock_socket.return_value.__enter__.return_value = mock_sock
            
            port = find_available_port(8000)
            assert port == 8001
            assert mock_sock.connect_ex.call_count == 2

    def test_get_running_container_endpoint_found(self):
        """Test getting endpoint when container is running."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.shutil.which') as mock_which, \
             patch('subprocess.run') as mock_run:
            mock_which.return_value = "/usr/local/bin/docker"
            
            # Mock the three subprocess calls in order
            mock_run.side_effect = [
                MagicMock(stdout="container_id"),  # ps -a -q (container exists)
                MagicMock(stdout="container_id"),  # ps -q (container is running)
                MagicMock(stdout="0.0.0.0:8001->8000/tcp")  # ps --format (get ports)
            ]
            
            endpoint = get_running_container_endpoint()
            assert endpoint == "http://localhost:8001"
            
            mock_which.assert_called_once_with("docker")
            assert mock_run.call_count == 3

    def test_get_running_container_endpoint_not_found(self):
        """Test getting endpoint when no container is running."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.shutil.which') as mock_which, \
             patch('subprocess.run') as mock_run:
            mock_which.return_value = "/usr/local/bin/docker"
            mock_run.return_value.stdout = ""
            
            endpoint = get_running_container_endpoint()
            assert endpoint is None

    def test_get_running_container_endpoint_stopped_container(self):
        """Test getting endpoint when container exists but is stopped."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.shutil.which') as mock_which, \
             patch('subprocess.run') as mock_run:
            mock_which.return_value = "/usr/local/bin/docker"
            
            # Mock the subprocess calls: container exists, not running, then restart and get ports
            mock_run.side_effect = [
                MagicMock(stdout="container_id"),  # ps -a -q (container exists)
                MagicMock(stdout=""),  # ps -q (container not running)
                MagicMock(stdout=""),  # docker start (restart container)
                MagicMock(stdout="0.0.0.0:8002->8000/tcp")  # ps --format (get ports after restart)
            ]
            
            endpoint = get_running_container_endpoint()
            assert endpoint == "http://localhost:8002"
            
            mock_which.assert_called_once_with("docker")
            assert mock_run.call_count == 4

    def test_start_docker_container_success(self):
        """Test Docker container start success."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.shutil.which') as mock_which, \
             patch('subprocess.run') as mock_run, \
             patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.boto3.client') as mock_boto3_client:
            mock_which.return_value = "/usr/local/bin/docker"
            mock_run.return_value = MagicMock()
            
            # Mock boto3 client and list_tables call
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client
            mock_client.list_tables.return_value = {'TableNames': []}
            
            endpoint = start_docker_container(8000)
            assert endpoint == "http://localhost:8000"
            
            mock_which.assert_called_once_with("docker")
            mock_run.assert_called_once_with([
                "/usr/local/bin/docker", "run", "-d", "--name", "dynamodb-local-mcp-server",
                "-p", "8000:8000", "amazon/dynamodb-local"
            ], capture_output=True, text=True, check=True)
            mock_boto3_client.assert_called_with('dynamodb', endpoint_url="http://localhost:8000")
            mock_client.list_tables.assert_called_once()

    def test_setup_dynamodb_local_reuse_existing(self):
        """Test setup reuses existing container."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.get_running_container_endpoint') as mock_get_endpoint:
            mock_get_endpoint.return_value = "http://localhost:8001"
            
            endpoint = setup_dynamodb_local()
            assert endpoint == "http://localhost:8001"

    def test_setup_dynamodb_local_no_docker(self):
        """Test setup fails when Docker not available."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.get_running_container_endpoint') as mock_get_endpoint, \
             patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.is_docker_available') as mock_docker_available:
            mock_get_endpoint.return_value = None
            mock_docker_available.return_value = False
            
            with pytest.raises(RuntimeError) as exc_info:
                setup_dynamodb_local()
            
            assert "Docker is not available" in str(exc_info.value)

    def test_setup_dynamodb_local_new_container(self):
        """Test setup creates new container when none exists."""
        with patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.get_running_container_endpoint') as mock_get_endpoint, \
             patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.is_docker_available') as mock_docker_available, \
             patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.find_available_port') as mock_find_port, \
             patch('awslabs.dynamodb_mcp_server.dynamodb_local_setup.start_docker_container') as mock_start_docker:
            
            mock_get_endpoint.return_value = None
            mock_docker_available.return_value = True
            mock_find_port.return_value = 8001
            mock_start_docker.return_value = "http://localhost:8001"
            
            endpoint = setup_dynamodb_local()
            
            assert endpoint == "http://localhost:8001"
            mock_find_port.assert_called_once_with(8000)
            mock_start_docker.assert_called_once_with(8001)
