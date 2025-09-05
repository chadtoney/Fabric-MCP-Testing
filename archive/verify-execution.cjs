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

async function verifyNotebookExecution() {
    console.log('🔍 Verifying Notebook Execution Results');
    console.log('='.repeat(40));
    
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
        console.log('\n🚀 Checking lakehouse tables...');
        const tablesResult = await sendMCPRequest('get-fabric-lakehouse-tables', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId
        });
        
        const tablesText = tablesResult.content[0].text;
        console.log('\n📊 Lakehouse Tables:');
        console.log('='.repeat(30));
        console.log(tablesText);
        console.log('='.repeat(30));
        
        // Check if our expected tables exist
        const expectedTables = ['customers', 'products', 'sales_transactions', 'daily_website_metrics'];
        let foundTables = [];
        
        for (const table of expectedTables) {
            if (tablesText.toLowerCase().includes(table.toLowerCase())) {
                foundTables.push(table);
            }
        }
        
        console.log(`\n✅ Found ${foundTables.length}/${expectedTables.length} expected tables:`);
        foundTables.forEach(table => console.log(`  ✓ ${table}`));
        
        if (foundTables.length > 0) {
            console.log('\n🎉 SUCCESS! Notebook execution created tables in the lakehouse!');
            console.log('📊 Your comprehensive dataset is now available in Fabric');
            
            // Let's query one of the tables to see the data
            if (foundTables.includes('customers')) {
                console.log('\n🚀 Sampling customer data...');
                const queryResult = await sendMCPRequest('execute-livy-statement', {
                    bearerToken: token,
                    workspaceId: config.workspaceId,
                    lakehouseId: config.lakehouseId,
                    sessionId: await createSession(token),
                    code: 'spark.sql("SELECT COUNT(*) as customer_count FROM customers").show()',
                    kind: 'pyspark'
                });
                
                console.log('Customer count result:', queryResult.content[0].text);
            }
        } else {
            console.log('\n⚠️ No expected tables found. The execution may still be in progress.');
            console.log('💡 Try checking the Fabric portal directly to see if tables are being created.');
        }
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

async function createSession(token) {
    try {
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark',
                name: 'QuickVerification'
            }
        });
        
        const sessionData = JSON.parse(sessionResult.content[0].text);
        await new Promise(resolve => setTimeout(resolve, 20000)); // Wait for session
        return sessionData.id;
    } catch (error) {
        console.error('Failed to create session:', error.message);
        return null;
    }
}

verifyNotebookExecution().catch(console.error);
