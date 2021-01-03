PYTHON = python3
PROTOC = $(PYTHON) -m grpc_tools.protoc
PROTO_DIR = ./

init:
	sudo apt install protobuf-compiler

build:
	rm -f $(PROTO_DIR)/*_pb2.py
	$(PROTOC) -I=$(PROTO_DIR) --python_out=$(PROTO_DIR) $(PROTO_DIR)/*.proto

test: build
	$(PYTHON) dict_format_test.py