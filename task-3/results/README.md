# Задание 3. Реализация Distributed Scheduling с k8s CronJob

CronJob ежедневно в 20:00 UTC запускает [скрипт Python](export_shipments.py), который выгружает таблицу перевозок из PostgreSQL в CSV. Отчет сохраняется в постоянном томе и заменяется после успешной выгрузки. При ошибке предусмотрены два повтора.

## Запуск

```sh
cd task-3/results
minikube start
minikube image build -t shipment-export:1.0 .
kubectl create namespace task3
kubectl apply -n task3 -f k8s/
```

## Проверка

Ручной запуск:

```sh
kubectl create job -n task3 shipment-export-check --from=cronjob/shipment-export
kubectl wait -n task3 --for=condition=complete job/shipment-export-check --timeout=60s
kubectl logs -n task3 job/shipment-export-check
kubectl get -n task3 cronjobs,jobs,pods,pvc
kubectl exec -n task3 deployment/shipment-db -- cat /exports/shipments.csv
```
