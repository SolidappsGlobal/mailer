#!/bin/bash
# Script para fazer deploy no Cloud Run do Back4App

# Configurações
PROJECT_ID="seu-projeto-back4app"  # Substitua pelo ID do seu projeto
SERVICE_NAME="csv-processor"
REGION="us-central1"
IMAGE_NAME="gcr.io/$PROJECT_ID/$SERVICE_NAME"

echo "🚀 Iniciando deploy do CSV Processor para Cloud Run..."

# 1. Build da imagem Docker
echo "📦 Fazendo build da imagem Docker..."
docker build -t $IMAGE_NAME .

# 2. Push da imagem para Google Container Registry
echo "📤 Enviando imagem para GCR..."
docker push $IMAGE_NAME

# 3. Deploy no Cloud Run
echo "☁️ Fazendo deploy no Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_NAME \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 900 \
  --concurrency 100 \
  --max-instances 10 \
  --set-env-vars "BUBBLE_API_BASE_URL=https://myunitrust.com/version-live/api/1.1/obj" \
  --set-env-vars "BUBBLE_API_TOKEN=eafe2749ca27a1c37ccf000431c2d083" \
  --set-env-vars "BUBBLE_TABLE_NAME=prelicensingcsv" \
  --set-env-vars "BACK4APP_API_BASE_URL=https://parseapi.back4app.com/classes" \
  --set-env-vars "BACK4APP_APP_ID=mK60GEj1uzfoICD3dFxW75KZ5K77bbBoaWeeENeK" \
  --set-env-vars "BACK4APP_MASTER_KEY=ZDYmU9PLUhJRhTscXJGBFlU8wThrKY6Q0alTtZu2" \
  --set-env-vars "MAX_CONCURRENT=25" \
  --set-env-vars "CHUNK_SIZE=25" \
  --set-env-vars "RETRY_TOTAL=3" \
  --set-env-vars "BACKOFF_FACTOR=1.0"

echo "✅ Deploy concluído!"
echo "🌐 URL do serviço: https://$SERVICE_NAME-$REGION-$PROJECT_ID.a.run.app"
