const { spawn } = require('child_process');
const fs = require('fs');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    notebookId: "1df1becd-d296-4212-9e20-dceb390d3994"
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

async function sendMCPRequest(tool, params, timeout = 120000) {
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

async function deployNotebookContent() {
    console.log('📝 Deploying Notebook Content to Fabric');
    console.log('='.repeat(45));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    // Read the local notebook
    console.log('📖 Reading local notebook...');
    const notebookPath = 'fabric-lakehouse-data-population.ipynb';
    if (!fs.existsSync(notebookPath)) {
        console.error(`❌ Notebook not found: ${notebookPath}`);
        return;
    }
    
    const notebookContent = fs.readFileSync(notebookPath, 'utf8');
    console.log(`✅ Read notebook (${notebookContent.length} characters)`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Checking current notebook status...');
        const currentNotebook = await sendMCPRequest('get-fabric-notebook', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.notebookId
        });
        
        console.log('✅ Current notebook found');
        
        console.log('\n🚀 Getting notebook definition to understand the format...');
        const notebookDef = await sendMCPRequest('get-fabric-notebook-definition', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.notebookId,
            format: 'ipynb'
        });
        
        console.log('✅ Got notebook definition format');
        
        // The issue might be that we need to use the run-fabric-notebook to execute our content
        // instead of trying to update the definition. Let me try a different approach
        
        console.log('\n🚀 Alternative: Running the notebook content via Livy...');
        console.log('This will execute our data population code directly');
        
        // Parse our notebook to extract the Python code
        // This is a VS Code notebook format, not standard Jupyter
        let allCode = '';
        
        // Extract code from VS Code Cell format
        const cellMatches = notebookContent.match(/<VSCode\.Cell[^>]*language="python"[^>]*>([\s\S]*?)<\/VSCode\.Cell>/g);
        
        if (cellMatches) {
            for (const cellMatch of cellMatches) {
                // Extract the content between the tags
                const content = cellMatch.replace(/<VSCode\.Cell[^>]*>/, '').replace(/<\/VSCode\.Cell>/, '').trim();
                if (content) {
                    allCode += content + '\n\n';
                }
            }
        } else {
            console.log('⚠️ No Python cells found, trying standard Jupyter format...');
            try {
                const notebook = JSON.parse(notebookContent);
                for (const cell of notebook.cells) {
                    if (cell.cell_type === 'code' && cell.source) {
                        allCode += (Array.isArray(cell.source) ? cell.source.join('\n') : cell.source) + '\n\n';
                    }
                }
            } catch (e) {
                console.error('❌ Failed to parse notebook format');
            }
        }
        
        console.log(`✅ Extracted ${allCode.length} characters of Python code`);
        
        if (allCode.length === 0) {
            console.error('❌ No Python code found in notebook');
            return;
        }
        
        // Now let's execute this via Livy session
        console.log('\n🚀 Creating Livy session to run the notebook code...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61",
            sessionConfig: {
                kind: 'pyspark',
                name: 'NotebookContentExecution'
            }
        });
        
        const sessionData = JSON.parse(sessionResult.content[0].text);
        const sessionId = sessionData.id;
        console.log(`✅ Created session: ${sessionId}`);
        
        console.log('⏳ Waiting 30 seconds for session to initialize...');
        await new Promise(resolve => setTimeout(resolve, 30000));
        
        console.log('\n🚀 Executing the full notebook code...');
        const execResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61",
            sessionId: sessionId,
            code: allCode,
            kind: 'pyspark'
        });
        
        const resultText = execResult.content[0].text;
        console.log('\n📋 Execution Result:');
        console.log('='.repeat(50));
        console.log(resultText);
        console.log('='.repeat(50));
        
        if (resultText.includes('SUCCESS') || resultText.includes('completed')) {
            console.log('\n🎉 SUCCESS! Notebook content executed successfully!');
            console.log('📊 Your lakehouse should now be populated with the complete dataset');
            console.log('🔍 Check Fabric Portal to see the new tables and data');
        } else {
            console.log('\n⚠️ Execution completed - check output above for results');
        }
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

deployNotebookContent().catch(console.error);
