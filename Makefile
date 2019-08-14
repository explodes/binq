proto:
	for x in *.proto; do protoc --go_out=paths=source_relative,plugins=grpc:. $$x; done