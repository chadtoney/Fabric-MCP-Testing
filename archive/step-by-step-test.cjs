const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

let mcpProcess;

async function sendMCPRequest(tool, params) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error('Request timeout'));
        }, 30000);

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
        
        let fullResponse = '';
        let responseReceived = false;

        const dataHandler = (data) => {
            if (responseReceived) return;
            
            fullResponse += data.toString();
            
            try {
                // Look for complete JSON responses
                const lines = fullResponse.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === requestId) {
                            responseReceived = true;
                            clearTimeout(timeout);
                            
                            if (parsed.error) {
                                reject(new Error(`${parsed.error.code}: ${parsed.error.message}`));
                            } else {
                                resolve(parsed.result);
                            }
                            return;
                        }
                    }
                }
            } catch (e) {
                // Continue collecting data
            }
        };

        mcpProcess.stdout.on('data', dataHandler);
        mcpProcess.stderr.on('data', dataHandler);

        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

async function testStep() {
    console.log('🧪 Step-by-Step Authentication and Session Test');
    
    mcpProcess = spawn('node', ['build/index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: process.cwd()
    });

    // Wait for startup
    await new Promise(resolve => setTimeout(resolve, 3000));
    console.log('✅ MCP Server started\n');

    try {
        // Step 1: Check authentication status
        console.log('🔐 Step 1: Checking authentication...');
        const authStatus = await sendMCPRequest('check-fabric-auth-status', {});
        console.log('Auth status:', JSON.stringify(authStatus, null, 2));

        // Step 2: List workspaces using the newer tool
        console.log('\n📂 Step 2: Listing workspaces...');
        const workspaces = await sendMCPRequest('fabric_list_workspaces', {});
        console.log(`Found ${workspaces.value?.length || 0} workspaces`);
        
        // Find our workspace
        const targetWorkspace = workspaces.value?.find(w => w.id === config.workspaceId);
        if (targetWorkspace) {
            console.log(`✅ Found target workspace: ${targetWorkspace.displayName}`);
        } else {
            console.log('❌ Target workspace not found');
            return;
        }

        // Step 3: List current Livy sessions 
        console.log('\n🔍 Step 3: Listing current Livy sessions...');
        const sessions = await sendMCPRequest('list-livy-sessions', {
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId
        });
        console.log(`Found ${sessions.sessions?.length || 0} existing sessions`);
        sessions.sessions?.forEach((session, index) => {
            console.log(`  ${index + 1}. Session ${session.id}: ${session.state} (${session.kind})`);
        });

        // Step 4: Create new session with explicit parameters
        console.log('\n🆕 Step 4: Creating new Livy session...');
        const sessionConfig = {
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: "pyspark",
                name: "step-test-session",
                driverMemory: "4g",
                driverCores: 2,
                executorMemory: "2g", 
                executorCores: 2,
                numExecutors: 2
            }
        };
        
        console.log('Session config:', JSON.stringify(sessionConfig, null, 2));
        const newSession = await sendMCPRequest('create-livy-session', sessionConfig);
        console.log('✅ New session response:', JSON.stringify(newSession, null, 2));
        
        if (newSession.id) {
            console.log(`🎉 Successfully created session with ID: ${newSession.id}`);
            
            // Wait for session to be ready
            console.log('\n⏳ Step 5: Waiting for session to be ready...');
            await new Promise(resolve => setTimeout(resolve, 30000));
            
            // Check session status
            const sessionStatus = await sendMCPRequest('get-livy-session', {
                workspaceId: config.workspaceId,
                lakehouseId: config.lakehouseId,
                sessionId: newSession.id
            });
            console.log('Session status:', sessionStatus.state);
            
            if (sessionStatus.state === 'idle') {
                console.log('\n🚀 Step 6: Testing simple statement execution...');
                const statement = await sendMCPRequest('execute-livy-statement', {
                    workspaceId: config.workspaceId,
                    lakehouseId: config.lakehouseId,
                    sessionId: newSession.id,
                    code: 'print("Hello from new session!")',
                    kind: 'pyspark'
                });
                console.log('Statement submitted:', statement.id);
                
                // Wait and check result
                await new Promise(resolve => setTimeout(resolve, 10000));
                const result = await sendMCPRequest('get-livy-statement', {
                    workspaceId: config.workspaceId,
                    lakehouseId: config.lakehouseId,
                    sessionId: newSession.id,
                    statementId: statement.id
                });
                console.log('✅ Statement result:', result.state);
                if (result.output) {
                    console.log('Output:', result.output.data?.['text/plain'] || result.output);
                }
            }
        }

    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Stopping MCP server...');
    mcpProcess.kill();
}

testStep().catch(console.error);
