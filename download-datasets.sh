#!/usr/bin/env bash
set -e

echo "==> Preparando diretórios de dados..."
mkdir -p ./data/input
mkdir -p ./data/output

if [ ! -d "./data/input/dataset-json-clientes" ]; then
  echo "==> Baixando dataset de clientes..."
  git clone https://github.com/infobarbosa/dataset-json-clientes ./data/input/dataset-json-clientes
else
  echo "==> Dataset de clientes já existe em ./data/input/dataset-json-clientes"
fi

if [ ! -d "./data/input/datasets-csv-pedidos" ]; then
  echo "==> Baixando dataset de pedidos..."
  git clone https://github.com/infobarbosa/datasets-csv-pedidos ./data/input/datasets-csv-pedidos
else
  echo "==> Dataset de pedidos já existe em ./data/input/datasets-csv-pedidos"
fi

echo "==> Verificando arquivos baixados:"
echo "--- Clientes ---"
ls -lh ./data/input/dataset-json-clientes/data/
echo "--- Pedidos ---"
ls -lh ./data/input/datasets-csv-pedidos/data/pedidos/ | head -n 6

echo "==> Concluído com sucesso!"
