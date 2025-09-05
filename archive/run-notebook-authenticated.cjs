const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    notebookId: "1df1becd-d296-4212-9e20-dceb390d3994",
    serverCommand: "node",
    serverArgs: ["build/index.js"]
};

async function sendMCPRequest(toolName, args) {
    return new Promise((resolve, reject) => {
        let serverOutput = '';
        let client;
        let transport;

        try {
            // Set up environment for Azure CLI authentication
            const env = {
                ...process.env,
                FABRIC_AUTH_METHOD: 'azure_cli',
                FABRIC_DEFAULT_WORKSPACE_ID: config.workspaceId
            };

            console.log('🔐 Setting up Azure CLI authentication...');
            console.log(`   FABRIC_AUTH_METHOD: ${env.FABRIC_AUTH_METHOD}`);
            console.log(`   FABRIC_DEFAULT_WORKSPACE_ID: ${env.FABRIC_DEFAULT_WORKSPACE_ID}`);

            // Create transport and client with proper environment
            transport = new StdioClientTransport({
                command: config.serverCommand,
                args: config.serverArgs,
                stderr: 'pipe',
                env: env
            });

            client = new Client({
                name: "fabric-notebook-runner-authenticated",
                version: "1.0.0"
            }, {
                capabilities: {
                    tools: {}
                }
            });

            // Handle server stderr for debugging
            if (transport.process && transport.process.stderr) {
                transport.process.stderr.on('data', (data) => {
                    serverOutput += data.toString();
                    const output = data.toString();
                    
                    if (output.includes('Microsoft Fabric Analytics MCP Server running')) {
                        console.log('✅ MCP Server is ready');
                    }
                    if (output.includes('Azure CLI authentication')) {
                        console.log('🔐 Azure CLI authentication in progress...');
                    }
                    if (output.includes('Authentication successful')) {
                        console.log('✅ Authentication successful!');
                    }
                });
            }

            // Connect and make request
            client.connect(transport).then(async () => {
                console.log('🔗 Connected to authenticated MCP server');
                
                try {
                    const response = await client.request(
                        { method: 'tools/call', params: { name: toolName, arguments: args } },
                        { timeout: 120000 } // 2 minutes timeout
                    );
                    resolve(response);
                } catch (error) {
                    reject(error);
                } finally {
                    await client.close();
                }
            }).catch(reject);

        } catch (error) {
            reject(error);
        }
    });
}

async function runNotebookWithAuthentication() {
    console.log('🚀 Running notebook in Fabric with Azure CLI authentication...');
    console.log(`Workspace: ${config.workspaceId}`);
    console.log(`Notebook: ${config.notebookId}`);
    console.log('');

    try {
        // First, let's try to list workspaces to verify authentication
        console.log('🔍 Step 1: Verifying authentication by listing workspaces...');
        
        try {
            const workspacesResult = await sendMCPRequest('list-fabric-workspaces', {
                bearerToken: 'azure_cli' // This triggers Azure CLI authentication
            });
            console.log('✅ Authentication verified - workspaces accessible');
            
            if (workspacesResult.result && workspacesResult.result.content) {
                const responseText = workspacesResult.result.content[0].text;
                if (responseText.includes(config.workspaceId)) {
                    console.log('✅ Target workspace found in accessible workspaces');
                }
            }
        } catch (authError) {
            console.log('⚠️ Workspace listing failed:', authError.message);
            console.log('Proceeding with notebook execution anyway...');
        }

        console.log('\n📝 Step 2: Running notebook using run-fabric-notebook...');
        
        const runResult = await sendMCPRequest('run-fabric-notebook', {
            bearerToken: 'azure_cli', // This triggers Azure CLI authentication
            workspaceId: config.workspaceId,
            notebookId: config.notebookId,
            parameters: {},
            configuration: {
                timeout: 1800 // 30 minutes
            }
        });

        console.log('✅ Notebook execution started successfully!');
        console.log('Response:', JSON.stringify(runResult, null, 2));

        if (runResult.result && runResult.result.content) {
            const responseText = runResult.result.content[0].text;
            console.log('\n📋 Execution details:');
            console.log(responseText);

            // Extract job ID if available
            const jobIdMatch = responseText.match(/Job ID: ([a-f0-9-]+)/);
            if (jobIdMatch) {
                const jobId = jobIdMatch[1];
                console.log(`\n🔍 Monitor execution status:`);
                console.log(`https://app.fabric.microsoft.com/groups/${config.workspaceId}/`);
                console.log(`Job ID: ${jobId}`);
                
                // Try to check job status
                console.log('\n⏱️ Checking execution status...');
                setTimeout(async () => {
                    try {
                        const statusResult = await sendMCPRequest('get-fabric-job-status', {
                            workspaceId: config.workspaceId,
                            jobId: jobId
                        });
                        console.log('📊 Job status:', JSON.stringify(statusResult, null, 2));
                    } catch (statusError) {
                        console.log('⚠️ Could not check job status:', statusError.message);
                    }
                }, 5000);
            }
        }

        console.log('\n🎉 Notebook execution initiated successfully!');
        console.log('📝 The notebook will populate your lakehouse with:');
        console.log('   - demo_sales table (1,000 records)');
        console.log('   - product_performance_summary table');
        console.log('   - regional_sales_analysis table');
        console.log('   - monthly_sales_trends table');
        console.log('   - sales_channel_performance table');
        console.log('   - customer_insights table');

    } catch (runError) {
        console.log('⚠️ run-fabric-notebook failed, trying execute-fabric-notebook...');
        console.log('Error:', runError.message);

        try {
            const executeResult = await sendMCPRequest('execute-fabric-notebook', {
                bearerToken: 'azure_cli', // This triggers Azure CLI authentication  
                workspaceId: config.workspaceId,
                notebookId: config.notebookId,
                parameters: {},
                timeout: 1800
            });

            console.log('✅ Notebook execution started with execute method!');
            console.log('Response:', JSON.stringify(executeResult, null, 2));

            if (executeResult.result && executeResult.result.content) {
                const responseText = executeResult.result.content[0].text;
                console.log('\n📋 Execution details:');
                console.log(responseText);
            }

        } catch (executeError) {
            console.log('❌ Both execution methods failed');
            console.log('run-fabric-notebook error:', runError.message);
            console.log('execute-fabric-notebook error:', executeError.message);
            
            console.log('\n🔍 Troubleshooting steps:');
            console.log('1. Verify Azure CLI is logged in: az account show');
            console.log('2. Check Fabric access: az rest --method GET --url "https://api.fabric.microsoft.com/v1/workspaces" --resource "https://api.fabric.microsoft.com"');
            console.log('3. Verify notebook exists in workspace');
            console.log('4. Check if notebook has default lakehouse configured');
            
            console.log('\n🛠️ Manual execution fallback:');
            console.log('1. Open: https://app.fabric.microsoft.com/');
            console.log(`2. Navigate to workspace: ${config.workspaceId}`);
            console.log('3. Open "LakehouseDataPopulation" notebook');
            console.log('4. Set MCPDemoLakehouse as default lakehouse');
            console.log('5. Click "Run all" to execute all cells');
        }
    }
}

// Run the function
runNotebookWithAuthentication().catch(console.error);
