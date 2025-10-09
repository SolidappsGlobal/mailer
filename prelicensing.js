// Cloud Functions para Back4App - Migração do main.py
// Mantém todas as funcionalidades: filas, processamento assíncrono e logs estruturados

const Parse = require('parse/node');

// Configurações do Back4App (mesmo do main.py)
const BACK4APP_CONFIG = {
    API_BASE_URL: "https://parseapi.back4app.com/classes",
    APP_ID: "mK60GEj1uzfoICD3dFxW75KZ5K77bbBoaWeeENeK",
    MASTER_KEY: "ZDYmU9PLUhJRhTscXJGBFlU8wThrKY6Q0alTtZu2",
    TABLE_NAME: "API_Connector_Users",
    CSV_TABLE_NAME: "Prelicensingcsv"
};

// Configurações do sistema (mesmo do main.py)
const SYSTEM_CONFIG = {
    MAX_CONCURRENT: 25,
    CHUNK_SIZE: 25,
    RETRY_TOTAL: 3,
    BACKOFF_FACTOR: 1,
    STATUS_FORCELIST: [429, 500, 502, 503, 504],
    CSV_SIZE_THRESHOLD: 100000 // 100KB
};

// Headers para Back4App (mesmo do main.py)
const BACK4APP_HEADERS = {
    "X-Parse-Application-Id": BACK4APP_CONFIG.APP_ID,
    "X-Parse-Master-Key": BACK4APP_CONFIG.MASTER_KEY,
    "Content-Type": "application/json"
};

// ============================================================================
// ESTRATÉGIA 1: SISTEMA DE FILAS (Usando Back4App - mesmo do main.py)
// ============================================================================

// Função para adicionar item à fila (usando Parse SDK)
async function addToQueue(csvData) {
    try {
        const Prelicensingcsv = Parse.Object.extend(BACK4APP_CONFIG.CSV_TABLE_NAME);
        const obj = new Prelicensingcsv();
        
        obj.set("filename", csvData.csv_filename);
        obj.set("csv_url", csvData.csv_url);
        obj.set("csv_content", csvData.csv_content);
        obj.set("file_size", csvData.file_size);
        obj.set("total_records", csvData.total_records);
        obj.set("processed_records", csvData.processed_records || 0);
        obj.set("processing_status", csvData.processing_status);
        obj.set("source_email", csvData.source_email || "google_apps_script");
        obj.set("imo", csvData.imo || "");
        obj.set("queue_priority", csvData.queue_priority || 1);
        obj.set("created_at", new Date());
        
        const result = await obj.save(null, { useMasterKey: true });
        return result.id;
    } catch (error) {
        console.error("Error adding to queue:", error);
        throw error;
    }
}

// Função para buscar próximo item da fila (usando Parse SDK)
async function getNextQueueItem() {
    try {
        const Prelicensingcsv = Parse.Object.extend(BACK4APP_CONFIG.CSV_TABLE_NAME);
        const query = new Parse.Query(Prelicensingcsv);
        
        query.equalTo("processing_status", "queued");
        query.descending("queue_priority");
        query.ascending("created_at");
        query.limit(1);
        
        const result = await query.first({ useMasterKey: true });
        return result ? result.toJSON() : null;
    } catch (error) {
        console.error("Error getting next queue item:", error);
        return null;
    }
}

// Função para atualizar status do item da fila (usando Parse SDK)
async function updateQueueStatus(queueId, status, processedRecords = 0, errorMessage = null) {
    try {
        const Prelicensingcsv = Parse.Object.extend(BACK4APP_CONFIG.CSV_TABLE_NAME);
        const query = new Parse.Query(Prelicensingcsv);
        const obj = await query.get(queueId, { useMasterKey: true });
        
        obj.set("processing_status", status);
        obj.set("processed_records", processedRecords);
        
        if (errorMessage) {
            obj.set("error_message", errorMessage);
        }
        
        if (status === "completed") {
            obj.set("processed_at", new Date());
        }
        
        await obj.save(null, { useMasterKey: true });
    } catch (error) {
        console.error("Error updating queue status:", error);
        throw error;
    }
}

// ============================================================================
// ESTRATÉGIA 2: PROCESSAMENTO EM BACKGROUND (Usando Back4App)
// ============================================================================

// Função para processar CSV em background
async function processCsvBackground(csvData) {
    try {
        // Adicionar à fila
        const queueId = await addToQueue(csvData);
        
        // Processar em background usando setTimeout (simula thread)
        setTimeout(async () => {
            await processQueueItem(queueId);
        }, 100);
        
        return {
            success: true,
            queue_id: queueId,
            message: "Processing started in background",
            status: "queued"
        };
        
    } catch (error) {
        console.error("Error starting background processing:", error);
        return {
            success: false,
            error: error.message
        };
    }
}

// Função para processar item da fila (usando Parse SDK)
async function processQueueItem(queueId) {
    try {
        // Buscar item da fila
        const Prelicensingcsv = Parse.Object.extend(BACK4APP_CONFIG.CSV_TABLE_NAME);
        const query = new Parse.Query(Prelicensingcsv);
        const item = await query.get(queueId, { useMasterKey: true });
        
        if (!item) {
            throw new Error("Queue item not found");
        }
        
        // Atualizar status para processing
        await updateQueueStatus(queueId, 'processing');
        
        // Processar CSV
        const result = await processCsvData(item.toJSON());
        
        // Atualizar status para completed
        await updateQueueStatus(queueId, 'completed', result.total_processed);
        
        console.log(`Queue item ${queueId} processed successfully: ${result.total_processed} records`);
        
    } catch (error) {
        console.error(`Error processing queue item ${queueId}:`, error);
        
        // Atualizar status para error
        await updateQueueStatus(queueId, 'error', 0, error.message);
    }
}

// ============================================================================
// ESTRATÉGIA 3: LOGS ESTRUTURADOS (Usando console - mais simples)
// ============================================================================

// Sistema de logging estruturado (usando console)
class StructuredLogger {
    constructor() {
        this.source = 'csv_processor';
    }
    
    log(level, message, context = {}) {
        const timestamp = new Date().toISOString();
        const logEntry = {
            timestamp,
            level: level.toUpperCase(),
            message,
            context,
            source: this.source
        };
        
        // Log no console com formatação
        console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`, context);
        
        // Em produção, você pode salvar em Back4App se necessário
        // await this.saveToBack4App(logEntry);
    }
    
    info(message, context = {}) {
        this.log('info', message, context);
    }
    
    error(message, context = {}) {
        this.log('error', message, context);
    }
    
    warning(message, context = {}) {
        this.log('warning', message, context);
    }
    
    debug(message, context = {}) {
        this.log('debug', message, context);
    }
}

// ============================================================================
// FUNÇÕES PRINCIPAIS (Migradas do main.py - usando Back4App)
// ============================================================================

// Função para buscar CSV da URL (usando fetch)
async function fetchCsvFromUrl(url) {
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.text();
    } catch (error) {
        console.error(`Failed to fetch CSV from ${url}:`, error);
        throw error;
    }
}

// Função para processar dados do CSV (usando Back4App)
async function processCsvData(csvData) {
    const logger = new StructuredLogger();
    
    try {
        logger.info("Starting CSV processing", { 
            filename: csvData.filename,
            file_size: csvData.file_size 
        });
        
        // Parse CSV (simplificado para JavaScript)
        const lines = csvData.csv_content.split('\n');
        const headers = lines[0].split(',');
        const rows = [];
        
        for (let i = 1; i < lines.length; i++) {
            if (lines[i].trim()) {
                const values = lines[i].split(',');
                const row = {};
                headers.forEach((header, index) => {
                    row[header.trim()] = values[index] ? values[index].trim() : '';
                });
                rows.push(row);
            }
        }
        
        logger.info("CSV parsed successfully", { 
            total_rows: rows.length 
        });
        
        // Processar em chunks
        let totalProcessed = 0;
        let totalNew = 0;
        let totalUpdated = 0;
        
        for (let i = 0; i < rows.length; i += SYSTEM_CONFIG.CHUNK_SIZE) {
            const chunk = rows.slice(i, i + SYSTEM_CONFIG.CHUNK_SIZE);
            
            const result = await processChunk(chunk, logger);
            
            totalProcessed += result.processed;
            totalNew += result.new;
            totalUpdated += result.updated;
            
            logger.info("Chunk processed", {
                chunk_start: i,
                chunk_size: chunk.length,
                processed: result.processed,
                new: result.new,
                updated: result.updated
            });
        }
        
        logger.info("CSV processing completed", {
            total_processed: totalProcessed,
            total_new: totalNew,
            total_updated: totalUpdated
        });
        
        return {
            total_processed: totalProcessed,
            total_new: totalNew,
            total_updated: totalUpdated
        };
        
    } catch (error) {
        logger.error("CSV processing failed", { 
            error: error.message,
            stack: error.stack 
        });
        throw error;
    }
}

// Função para processar chunk de dados (usando Back4App)
async function processChunk(chunk, logger) {
    // Extrair emails para verificar registros existentes
    const emails = chunk
        .map(row => row.EmailAddress?.toLowerCase().trim())
        .filter(email => email);
    
    // Buscar registros existentes
    const existingRecords = await getExistingRecords(emails);
    
    let processed = 0;
    let newRecords = 0;
    let updatedRecords = 0;
    
    // Processar cada linha
    for (const row of chunk) {
        try {
            const email = row.EmailAddress?.toLowerCase().trim();
            if (!email) continue;
            
            const existing = existingRecords[email];
            const payload = createPayload(row);
            
            if (existing) {
                // Atualizar registro existente
                await updateRecord(existing.objectId, payload);
                updatedRecords++;
                logger.debug("Record updated", { email });
            } else {
                // Criar novo registro
                await createRecord(payload);
                newRecords++;
                logger.debug("Record created", { email });
            }
            
            processed++;
            
        } catch (error) {
            logger.error("Error processing row", { 
                error: error.message,
                row: row 
            });
        }
    }
    
    return {
        processed,
        new: newRecords,
        updated: updatedRecords
    };
}

// Função para buscar registros existentes (usando Parse SDK)
async function getExistingRecords(emails) {
    if (emails.length === 0) return {};
    
    try {
        const ApiConnectorUsers = Parse.Object.extend(BACK4APP_CONFIG.TABLE_NAME);
        const query = new Parse.Query(ApiConnectorUsers);
        
        // Criar query para buscar por emails usando $or
        const emailQueries = emails.map(email => 
            query.equalTo("pre_licensing_email_text", email)
        );
        
        const mainQuery = Parse.Query.or(...emailQueries);
        mainQuery.limit(1000);
        
        const results = await mainQuery.find({ useMasterKey: true });
        const records = {};
        
        results.forEach(row => {
            const email = row.get("pre_licensing_email_text")?.toLowerCase();
            if (email) {
                records[email] = row.toJSON();
            }
        });
        
        return records;
    } catch (error) {
        console.error("Error getting existing records:", error);
        return {};
    }
}

// Função para criar payload do registro (mesmo do main.py)
function createPayload(row) {
    return {
        first_name_text: row.FirstName || '',
        last_name_text: row.LastName || '',
        pre_licensing_email_text: row.EmailAddress?.toLowerCase().trim() || '',
        phone_text: sanitizePhone(row.Phone || ''),
        imo_custom_imo: row.Department || '',
        hiring_manager_text: row.HiringManager || '',
        pre_licensing_course_text: row.Course || '',
        prepared_to_pass_text: row['Prepared to Pass'] || '',
        time_spent_text: row.TimeSpent || '',
        date_enrolled_date: parseDate(row.DateEnrolled),
        pre_licensing_course_last_login_date: parseDate(row.LastLoggedIn),
        ple_date_completed_date: parseDate(row['PLE DateCompleted']),
        ple_complete_number: parseNumber(row['% PLE Complete']),
        percentage_prep_complete_number: parseNumber(row['% Prep Complete']),
        percentage_sim_complete_number: parseNumber(row['% Sim Complete'])
    };
}

// Funções auxiliares (mesmo do main.py)
function sanitizePhone(phone) {
    const digits = (phone || '').replace(/\D/g, '');
    return digits.slice(-10);
}

function parseDate(dateStr) {
    if (!dateStr || !dateStr.trim()) return null;
    
    try {
        const date = new Date(dateStr);
        if (isNaN(date.getTime())) return null;
        
        // Retornar no formato Back4App
        return { "__type": "Date", "iso": date.toISOString() };
    } catch (error) {
        return null;
    }
}

function parseNumber(numStr) {
    const num = parseFloat(numStr);
    return isNaN(num) ? null : num;
}

// Função para criar registro (usando Parse SDK)
async function createRecord(payload) {
    try {
        const ApiConnectorUsers = Parse.Object.extend(BACK4APP_CONFIG.TABLE_NAME);
        const obj = new ApiConnectorUsers();
        
        // Definir campos do payload
        for (const [key, value] of Object.entries(payload)) {
            obj.set(key, value);
        }
        
        const result = await obj.save(null, { useMasterKey: true });
        return result.toJSON();
    } catch (error) {
        console.error("Error creating record:", error);
        throw error;
    }
}

// Função para atualizar registro (usando Parse SDK)
async function updateRecord(recordId, payload) {
    try {
        const ApiConnectorUsers = Parse.Object.extend(BACK4APP_CONFIG.TABLE_NAME);
        const query = new Parse.Query(ApiConnectorUsers);
        const obj = await query.get(recordId, { useMasterKey: true });
        
        // Atualizar campos do payload
        for (const [key, value] of Object.entries(payload)) {
            obj.set(key, value);
        }
        
        const result = await obj.save(null, { useMasterKey: true });
        return result.toJSON();
    } catch (error) {
        console.error("Error updating record:", error);
        throw error;
    }
}

// ============================================================================
// CLOUD FUNCTIONS EXPORTADAS
// ============================================================================

// Função principal para processar CSV
Parse.Cloud.define("processCsv", async (request) => {
    const { csv_url, csv_filename } = request.params || {};
    
    if (!csv_url) {
        throw new Error("csv_url é obrigatório");
    }
    
    try {
        // Buscar conteúdo do CSV
        const csvContent = await fetchCsvFromUrl(csv_url);
        
        // Determinar se deve processar imediatamente ou em fila
        const shouldQueue = csvContent.length > SYSTEM_CONFIG.CSV_SIZE_THRESHOLD;
        
        if (shouldQueue) {
            // Processar em background
            const csvData = {
                csv_url,
                csv_filename: csv_filename || 'unknown.csv',
                csv_content: '', // Não salvar conteúdo grande
                file_size: csvContent.length,
                total_records: csvContent.split('\n').length - 1,
                processing_status: 'queued',
                imo: extractImo(csvContent),
                queue_priority: 1
            };
            
            const result = await processCsvBackground(csvData);
            
            return {
                success: true,
                message: "Processing started in background",
                status: "queued",
                queue_id: result.queue_id,
                csv_url,
                note: "Check csv_processing_queue table for status"
            };
            
        } else {
            // Processar imediatamente
            const csvData = {
                csv_url,
                filename: csv_filename || 'unknown.csv',
                csv_content: csvContent,
                file_size: csvContent.length,
                total_records: csvContent.split('\n').length - 1,
                processing_status: 'processing',
                imo: extractImo(csvContent),
                queue_priority: 1
            };
            
            const result = await processCsvData(csvData);
            
            return {
                success: true,
                message: "Processing completed",
                status: "completed",
                total_processed: result.total_processed,
                total_new: result.total_new,
                total_updated: result.total_updated
            };
        }
        
    } catch (error) {
        console.error("Error processing CSV:", error);
        return {
            success: false,
            error: error.message
        };
    }
});

// Função para verificar status da fila (usando Parse SDK)
Parse.Cloud.define("getQueueStatus", async (request) => {
    const { queue_id } = request.params || {};
    
    try {
        const Prelicensingcsv = Parse.Object.extend(BACK4APP_CONFIG.CSV_TABLE_NAME);
        const query = new Parse.Query(Prelicensingcsv);
        
        if (queue_id) {
            // Buscar item específico
            const item = await query.get(queue_id, { useMasterKey: true });
            return {
                success: true,
                queue_items: item ? [item.toJSON()] : []
            };
        } else {
            // Buscar últimos 10 itens
            query.descending("createdAt");
            query.limit(10);
            
            const results = await query.find({ useMasterKey: true });
            return {
                success: true,
                queue_items: results.map(item => item.toJSON())
            };
        }
        
    } catch (error) {
        return {
            success: false,
            error: error.message
        };
    }
});

// Função para processar próximo item da fila (para workers)
Parse.Cloud.define("processNextQueueItem", async (request) => {
    try {
        const queueItem = await getNextQueueItem();
        
        if (!queueItem) {
            return {
                success: true,
                message: "No items in queue",
                queue_item: null
            };
        }
        
        // Processar item
        await processQueueItem(queueItem.objectId);
        
        return {
            success: true,
            message: "Queue item processed",
            queue_item: queueItem
        };
        
    } catch (error) {
        return {
            success: false,
            error: error.message
        };
    }
});

// Função auxiliar para extrair IMO do CSV (mesmo do main.py)
function extractImo(csvContent) {
    try {
        const lines = csvContent.split('\n');
        if (lines.length > 1) {
            const firstLine = lines[1]; // Segunda linha (primeira linha de dados)
            const columns = firstLine.split(',');
            return columns[0] || ''; // Primeira coluna é Department
        }
    } catch (error) {
        console.error("Error extracting IMO:", error);
    }
    return '';
}

// ============================================================================
// EXEMPLOS DE USO (Back4App Cloud Functions)
// ============================================================================

// Exemplo 1: Processar CSV pequeno (processamento imediato)
// Parse.Cloud.run("processCsv", {
//   csv_url: "https://example.com/small.csv",
//   csv_filename: "small_file.csv"
// });

// Exemplo 2: Processar CSV grande (processamento em background)
// Parse.Cloud.run("processCsv", {
//   csv_url: "https://example.com/large.csv",
//   csv_filename: "large_file.csv"
// });

// Exemplo 3: Verificar status da fila
// Parse.Cloud.run("getQueueStatus");

// Exemplo 4: Verificar status de item específico
// Parse.Cloud.run("getQueueStatus", { queue_id: "abc123" });

// Exemplo 5: Processar próximo item da fila (para workers)
// Parse.Cloud.run("processNextQueueItem");

// ============================================================================
// NOTAS IMPORTANTES
// ============================================================================

// 1. Este arquivo usa Back4App em vez de PostgreSQL
// 2. Mantém a mesma lógica do main.py
// 3. Usa Parse SDK para operações CRUD (não mais Parse.Cloud.httpRequest)
// 4. Sistema de filas usa tabela Prelicensingcsv
// 5. Logs são estruturados no console
// 6. Processamento assíncrono com setTimeout
// 7. Usa fetch() para chamadas HTTP externas (CSV)
// 8. Todas as operações Back4App usam Parse SDK nativo
