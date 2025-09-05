const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli', // This will trigger Azure CLI authentication
  lakehouseName: 'MCP Demo Lakehouse',
  lakehouseDescription: 'Demo lakehouse created via MCP server with sample data'
};

console.log('🚀 Starting Microsoft Fabric MCP Lakehouse Demo');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Authentication: Azure CLI`);

// Start the MCP server process
const mcpServer = spawn('node', ['build/index.js'], {
  stdio: ['pipe', 'pipe', 'pipe'],
  env: {
    ...process.env,
    FABRIC_AUTH_METHOD: 'azure_cli',
    FABRIC_DEFAULT_WORKSPACE_ID: config.workspaceId
  }
});

let requestId = 1;
let responseBuffer = '';

function sendMcpRequest(method, params = {}) {
  const request = {
    jsonrpc: '2.0',
    id: requestId++,
    method: method,
    params: params
  };
  
  console.log(`📤 Sending MCP request: ${method}`);
  console.log(`📝 Request params:`, JSON.stringify(params, null, 2));
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Request ${method} timed out after 30 seconds`));
    }, 30000);
    
    function processResponse(data) {
      responseBuffer += data.toString();
      const lines = responseBuffer.split('\n');
      
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i].trim();
        if (line) {
          try {
            const response = JSON.parse(line);
            if (response.id === request.id) {
              clearTimeout(timeout);
              mcpServer.stdout.removeListener('data', processResponse);
              responseBuffer = lines[lines.length - 1]; // Keep the last incomplete line
              resolve(response);
              return;
            }
          } catch (error) {
            // Ignore JSON parse errors for incomplete lines
            console.log('🔍 Parsing response line:', line);
          }
        }
      }
      responseBuffer = lines[lines.length - 1]; // Keep the last incomplete line
    }
    
    mcpServer.stdout.on('data', processResponse);
  });
}

async function createLakehouseDemo() {
  try {
    console.log('\n📋 Step 1: Listing existing items...');
    const listResponse = await sendMcpRequest('tools/call', {
      name: 'list-fabric-items',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        itemType: 'Lakehouse'
      }
    });
    
    console.log('✅ List response:', JSON.stringify(listResponse, null, 2));
    
    console.log('\n🏗️ Step 2: Creating new lakehouse...');
    const createResponse = await sendMcpRequest('tools/call', {
      name: 'create-fabric-item',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        itemType: 'Lakehouse',
        displayName: config.lakehouseName,
        description: config.lakehouseDescription
      }
    });
    
    console.log('✅ Create response:', JSON.stringify(createResponse, null, 2));
    
    // Extract lakehouse ID if successful
    if (createResponse.result && createResponse.result.content) {
      const content = createResponse.result.content[0].text;
      const idMatch = content.match(/ID: ([a-f0-9-]+)/);
      if (idMatch) {
        const lakehouseId = idMatch[1];
        console.log(`🎯 Created lakehouse with ID: ${lakehouseId}`);
        
        console.log('\n📊 Step 3: Creating sample notebook for data generation...');
        const notebookResponse = await sendMcpRequest('tools/call', {
          name: 'create-fabric-notebook',
          arguments: {
            bearerToken: config.bearerToken,
            workspaceId: config.workspaceId,
            displayName: 'Sample Data Generator',
            description: 'Notebook to create sample data in the lakehouse'
          }
        });
        
        console.log('✅ Notebook creation response:', JSON.stringify(notebookResponse, null, 2));
        
        if (notebookResponse.result && notebookResponse.result.content) {
          const notebookContent = notebookResponse.result.content[0].text;
          const notebookIdMatch = notebookContent.match(/ID: ([a-f0-9-]+)/);
          if (notebookIdMatch) {
            const notebookId = notebookIdMatch[1];
            console.log(`📓 Created notebook with ID: ${notebookId}`);
            
            console.log('\n🔄 Step 4: Starting Livy session for data operations...');
            const sessionResponse = await sendMcpRequest('tools/call', {
              name: 'create-livy-session',
              arguments: {
                bearerToken: config.bearerToken,
                workspaceId: config.workspaceId,
                lakehouseId: lakehouseId,
                sessionConfig: {
                  kind: 'pyspark',
                  name: 'demo-data-session',
                  driverMemory: '4g',
                  executorMemory: '2g',
                  numExecutors: 2
                }
              }
            });
            
            console.log('✅ Livy session response:', JSON.stringify(sessionResponse, null, 2));
          }
        }
        
        console.log('\n🎉 Demo completed successfully!');
        console.log(`Created lakehouse: ${config.lakehouseName} (ID: ${lakehouseId})`);
        console.log(`Workspace: https://msit.powerbi.com/groups/${config.workspaceId}`);
        console.log('\n📍 Next steps:');
        console.log('1. Open the lakehouse in Microsoft Fabric');
        console.log('2. Use the notebook to create sample data');
        console.log('3. Run Spark queries against the data');
      }
    }
    
  } catch (error) {
    console.error('❌ Error in demo:', error.message);
    console.error('Stack:', error.stack);
  } finally {
    console.log('\n🛑 Stopping MCP server...');
    mcpServer.kill();
    setTimeout(() => process.exit(0), 1000);
  }
}

// Handle MCP server startup
mcpServer.stderr.on('data', (data) => {
  const message = data.toString();
  console.log('🔧 MCP Server:', message.trim());
  
  if (message.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server started successfully');
    setTimeout(createLakehouseDemo, 2000); // Give server time to fully initialize
  }
});

mcpServer.stdout.on('data', (data) => {
  const message = data.toString();
  // Only log non-JSON messages to avoid cluttering the output
  if (!message.trim().startsWith('{')) {
    console.log('📤 MCP Output:', message.trim());
  }
});

mcpServer.on('error', (error) => {
  console.error('❌ MCP Server error:', error);
  process.exit(1);
});

mcpServer.on('exit', (code) => {
  console.log(`MCP Server exited with code ${code}`);
});
