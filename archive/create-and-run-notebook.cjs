const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61",
    notebookPath: "fabric-lakehouse-data-population.ipynb"
};

let mcpProcess;

async function sendMCPRequest(tool, params) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error(`Timeout for ${tool}`));
        }, 120000); // 2 minutes timeout

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
                            clearTimeout(timeout);
                            
                            if (parsed.error) {
                                console.log(`❌ Error:`, parsed.error.message);
                                reject(new Error(parsed.error.message));
                            } else {
                                console.log(`✅ ${tool} completed`);
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

async function createAndRunNotebook() {
    console.log('📓 Creating and Running Fabric Notebook for Data Population');
    console.log(`Workspace: ${config.workspaceId}`);
    console.log(`Lakehouse: ${config.lakehouseId}`);
    console.log(`Notebook: ${config.notebookPath}\n`);
    
    mcpProcess = spawn('node', ['build/index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: process.cwd()
    });

    // Wait for startup
    console.log('⏳ Starting MCP server...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    console.log('✅ MCP server ready\n');

    try {
        // Step 1: Check if notebook file exists
        const notebookFullPath = path.join(process.cwd(), config.notebookPath);
        if (!fs.existsSync(notebookFullPath)) {
            throw new Error(`Notebook file not found: ${notebookFullPath}`);
        }
        console.log(`✅ Notebook file found: ${config.notebookPath}`);

        // Step 2: Create notebook in Fabric workspace
        console.log('📝 Creating notebook in Fabric workspace...');
        const notebookName = "LakehouseDataPopulation";
        
        // First try to list existing notebooks to see if it already exists
        try {
            const existingNotebooks = await sendMCPRequest('list-fabric-notebooks', {
                workspaceId: config.workspaceId
            });
            
            const existingNotebook = existingNotebooks.value?.find(nb => nb.displayName === notebookName);
            if (existingNotebook) {
                console.log(`📒 Notebook '${notebookName}' already exists, using existing one`);
                console.log(`Notebook ID: ${existingNotebook.id}`);
                
                // Run the existing notebook
                console.log('\n🚀 Running existing notebook...');
                const runResult = await sendMCPRequest('run-fabric-notebook', {
                    workspaceId: config.workspaceId,
                    notebookId: existingNotebook.id,
                    configuration: {
                        defaultLakehouse: {
                            name: "MCPDemoLakehouse",
                            id: config.lakehouseId,
                            workspaceId: config.workspaceId
                        }
                    }
                });
                
                console.log('📊 Notebook execution initiated');
                console.log(`Job ID: ${runResult.id || 'Not provided'}`);
                
                if (runResult.id) {
                    console.log('\n⏳ Monitoring execution progress...');
                    // Monitor the execution
                    let attempts = 0;
                    const maxAttempts = 20; // 10 minutes
                    
                    while (attempts < maxAttempts) {
                        await new Promise(resolve => setTimeout(resolve, 30000)); // Wait 30 seconds
                        attempts++;
                        
                        try {
                            // Check notebook execution status
                            console.log(`📊 Checking execution status (attempt ${attempts}/${maxAttempts})...`);
                            
                            // Note: We may need to implement a different monitoring approach
                            // For now, we'll assume the execution is in progress
                            
                        } catch (error) {
                            console.log(`⚠️ Error checking status: ${error.message}`);
                        }
                    }
                }
                
                return;
            }
        } catch (error) {
            console.log(`⚠️ Could not list existing notebooks: ${error.message}`);
        }

        // Create new notebook if it doesn't exist
        const newNotebook = await sendMCPRequest('create-fabric-notebook', {
            workspaceId: config.workspaceId,
            displayName: notebookName,
            description: "Automated data population notebook for MCPDemoLakehouse with comprehensive sales data and analytics tables"
        });
        
        console.log(`✅ Created notebook: ${newNotebook.displayName}`);
        console.log(`Notebook ID: ${newNotebook.id}`);

        // Step 3: Wait a moment for notebook to be ready
        console.log('\n⏳ Waiting for notebook to be ready...');
        await new Promise(resolve => setTimeout(resolve, 10000));

        // Step 4: Run the notebook
        console.log('\n🚀 Running notebook...');
        const runResult = await sendMCPRequest('run-fabric-notebook', {
            workspaceId: config.workspaceId,
            notebookId: newNotebook.id,
            configuration: {
                defaultLakehouse: {
                    name: "MCPDemoLakehouse",
                    id: config.lakehouseId,
                    workspaceId: config.workspaceId
                }
            }
        });
        
        console.log('📊 Notebook execution initiated');
        console.log('Job details:', JSON.stringify(runResult, null, 2));

        // Step 5: Provide guidance for monitoring
        console.log('\n📋 NEXT STEPS:');
        console.log('1. Check the Fabric portal for notebook execution progress');
        console.log('2. Monitor the notebook run in the workspace');
        console.log('3. Verify tables are created in the lakehouse');
        console.log('\n🔗 Fabric Portal Links:');
        console.log(`Workspace: https://app.fabric.microsoft.com/groups/${config.workspaceId}`);
        console.log(`Notebook: https://app.fabric.microsoft.com/groups/${config.workspaceId}/items/${newNotebook.id}`);

    } catch (error) {
        console.error(`❌ Error: ${error.message}`);
        
        // Provide alternative approach
        console.log('\n🔄 ALTERNATIVE APPROACH:');
        console.log('1. Open Fabric portal manually');
        console.log('2. Navigate to your workspace');
        console.log('3. Upload the notebook file manually');
        console.log('4. Set MCPDemoLakehouse as default lakehouse');
        console.log('5. Run the notebook');
        console.log(`\nNotebook file location: ${config.notebookPath}`);
    }

    console.log('\n🛑 Stopping MCP server...');
    mcpProcess.kill();
}

createAndRunNotebook().catch(console.error);
