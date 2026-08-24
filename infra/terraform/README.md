# Infraestrutura

```
main.tf            S3 + Glue + Athena
lambdas.tf         fx, silver, gold
orchestration.tf   Step Functions + Scheduler
monitoring.tf      alarmes + dashboard
modules/lambda/    role + log group + função, usado 3×
```

## Rodar

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform apply
```

Requer os zips em `../dist/` (`scripts/build_lambdas.sh`).

## Notas

- Cada lambda tem role própria, com `Deny` de escrita em `bronze/*`.
- `Retry` só em erro transitório de infra — falha de Data Quality não é retentada.
- Alarmes sem destino (`alarm_actions = []`): não há canal de alerta neste ambiente.
- State local.
