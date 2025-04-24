protoc --python_out=. client_handler.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. client_handler.proto