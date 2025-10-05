# CSV Processor - Cloud Run

Este é o processador de CSV que salva dados simultaneamente no Bubble e no Back4App.

## 🚀 Deploy no Cloud Run

### Pré-requisitos:
- Google Cloud SDK instalado
- Docker instalado
- Projeto Google Cloud configurado
- Permissões para Cloud Run

### Passos para deploy:

1. **Configure o projeto:**
```bash
# Substitua pelo ID do seu projeto Back4App
export PROJECT_ID="seu-projeto-back4app"
gcloud config set project $PROJECT_ID
```

2. **Execute o deploy:**
```bash
# Torne o script executável
chmod +x deploy.sh

# Execute o deploy
./deploy.sh
```

### Deploy manual (alternativo):

```bash
# 1. Build da imagem
docker build -t gcr.io/$PROJECT_ID/csv-processor .

# 2. Push para GCR
docker push gcr.io/$PROJECT_ID/csv-processor

# 3. Deploy no Cloud Run
gcloud run deploy csv-processor \
  --image gcr.io/$PROJECT_ID/csv-processor \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 900 \
  --set-env-vars "BUBBLE_API_BASE_URL=https://myunitrust.com/version-live/api/1.1/obj" \
  --set-env-vars "BUBBLE_API_TOKEN=eafe2749ca27a1c37ccf000431c2d083" \
  --set-env-vars "BUBBLE_TABLE_NAME=prelicensingcsv" \
  --set-env-vars "BACK4APP_API_BASE_URL=https://parseapi.back4app.com/classes" \
  --set-env-vars "BACK4APP_APP_ID=mK60GEj1uzfoICD3dFxW75KZ5K77bbBoaWeeENeK" \
  --set-env-vars "BACK4APP_MASTER_KEY=ZDYmU9PLUhJRhTscXJGBFlU8wThrKY6Q0alTtZu2"
```

## 📋 Variáveis de Ambiente

| Variável | Descrição | Obrigatório |
|----------|-----------|-------------|
| `BUBBLE_API_BASE_URL` | URL base da API Bubble | ✅ |
| `BUBBLE_API_TOKEN` | Token de autenticação Bubble | ✅ |
| `BUBBLE_TABLE_NAME` | Nome da tabela Bubble | ✅ |
| `BACK4APP_API_BASE_URL` | URL base Back4App | ❌ (tem padrão) |
| `BACK4APP_APP_ID` | App ID Back4App | ❌ (tem padrão) |
| `BACK4APP_MASTER_KEY` | Master Key Back4App | ❌ (tem padrão) |

## 🧪 Testando

Após o deploy, teste com:

```bash
curl -X POST https://seu-servico.a.run.app \
  -H "Content-Type: application/json" \
  -H "bubble: eafe2749ca27a1c37ccf000431c2d083" \
  -d '{"csvfile": "https://exemplo.com/dados.csv"}'
```

## 📊 Funcionalidades

- ✅ Processa arquivos CSV via URL
- ✅ Salva dados no Bubble
- ✅ Salva dados no Back4App
- ✅ Atualiza registros existentes
- ✅ Retry automático com backoff
- ✅ Logs detalhados
- ✅ Processamento assíncrono

## 🔧 Configuração do Google Apps Script

Para usar com o Google Apps Script, atualize a URL:

```javascript
const API2_URL = 'https://seu-servico-back4app.a.run.app/enqueue_csv';
```
