# Airflow MySQL to BigQuery Pipelines

Este repositório contém pipelines desenvolvidos com **Apache Airflow** para transferir e transformar dados de um banco de dados **MySQL** para o **Google BigQuery**.

## 🚀 Objetivo

Substituir processos atuais de integração de dados (como o Data Fusion) por pipelines orquestrados com Airflow, garantindo:
- Flexibilidade e personalização.
- Escalabilidade e monitoramento.
- Facilidade de manutenção e versionamento de pipelines.

## ⚙️ Funcionalidades

- Conexão ao banco MySQL para extração de dados.
- Transformações simples e personalizadas em Python.
- Carregamento dos dados no BigQuery usando os operadores do Airflow.
- Agendamento de execuções periódicas.
- Monitoramento via UI do Airflow e logs detalhados.

## 🛠️ Tecnologias e Ferramentas

- [Apache Airflow](https://airflow.apache.org/)
- [Google Cloud BigQuery](https://cloud.google.com/bigquery)
- [MySQL](https://www.mysql.com/)
- [Google Cloud SDK](https://cloud.google.com/sdk) (para autenticação e uso de serviços GCP)


## 🏗️ Como rodar o projeto

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/renan-lemes/pipelines-Airflow
   cd pipelines-Airflow
