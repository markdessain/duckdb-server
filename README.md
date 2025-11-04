# duckdb-server

## Run and test connection

```bash
go run -tags=duckdb_arrow main.go -db data.duck
```

```bash 
go run client/main.go
```

## Run in a local k8s cluster

```bash
minikube start --cpus='4' --memory '10g'
kubectl config use-context minikube
minikube kubectl -- apply -f data.yaml
```