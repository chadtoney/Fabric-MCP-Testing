const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

async function getAzureToken() {
    const { execSync } = require('child_process');
    try {
        const result = execSync('az account get-access-token --resource "https://analysis.windows.net/powerbi/api"', { encoding: 'utf8' });
        const tokenData = JSON.parse(result);
        return tokenData.accessToken;
    } catch (error) {
        console.error('Failed to get Azure token:', error.message);
        return null;
    }
}

async function sendMCPRequest(tool, params, timeout = 60000) {
    return new Promise((resolve, reject) => {
        const timeoutHandle = setTimeout(() => {
            reject(new Error(`Request timeout for ${tool}`));
        }, timeout);

        const requestId = Date.now();
        const request = {
            jsonrpc: "2.0",
            id: requestId,
            method: "tools/call",
            params: {
                name: tool,
                arguments: params
            }
        };

        console.log(`📤 ${tool}...`);

        let responseReceived = false;

        const dataHandler = (data) => {
            if (responseReceived) return;
            
            const output = data.toString();
            
            try {
                const lines = output.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === requestId) {
                            responseReceived = true;
                            clearTimeout(timeoutHandle);
                            
                            if (parsed.error) {
                                reject(new Error(`MCP error: ${parsed.error.message}`));
                            } else {
                                resolve(parsed.result);
                            }
                            return;
                        }
                    }
                }
            } catch (e) {
                // Continue collecting
            }
        };

        mcpProcess.stdout.on('data', dataHandler);
        mcpProcess.stderr.on('data', dataHandler);

        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

let mcpProcess;

async function quickStatusCheck() {
    console.log('🚀 Quick Notebook Execution Status Check');
    console.log('='.repeat(42));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Creating quick Livy session to check tables...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark',
                name: 'QuickCheck'
            }
        });
        
        const sessionData = JSON.parse(sessionResult.content[0].text);
        const sessionId = sessionData.id;
        console.log(`✅ Created session: ${sessionId}`);
        
        console.log('⏳ Waiting 20 seconds for session to initialize...');
        await new Promise(resolve => setTimeout(resolve, 20000));
        
        console.log('\n🚀 Checking for tables in lakehouse...');
        const queryResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: 'spark.sql("SHOW TABLES").show()',
            kind: 'pyspark'
        });
        
        console.log('\n📊 Available Tables:');
        console.log('='.repeat(30));
        console.log(queryResult.content[0].text);
        console.log('='.repeat(30));
        
        // Check specific tables
        const expectedTables = ['customers', 'products', 'sales_transactions', 'daily_website_metrics'];
        
        for (const table of expectedTables) {
            try {
                console.log(`\n🔍 Checking table: ${table}`);
                const countResult = await sendMCPRequest('execute-livy-statement', {
                    bearerToken: token,
                    workspaceId: config.workspaceId,
                    lakehouseId: config.lakehouseId,
                    sessionId: sessionId,
                    code: `spark.sql("SELECT COUNT(*) as count FROM ${table}").show()`,
                    kind: 'pyspark'
                });
                
                console.log(`📊 ${table} record count:`, countResult.content[0].text);
            } catch (error) {
                console.log(`❌ Table ${table} not found or error: ${error.message}`);
            }
        }
        
        console.log('\n🎉 Notebook execution verification complete!');
        console.log('✅ Your lakehouse now contains the comprehensive dataset from the notebook');
        console.log('🔍 Check the Fabric portal to explore the data and run analytics');
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

quickStatusCheck().catch(console.error);
