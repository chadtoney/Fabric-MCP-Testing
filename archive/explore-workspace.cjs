const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli'
};

console.log('🔍 Exploring Microsoft Fabric workspace via MCP server');
console.log(`Workspace: ${config.workspaceId}`);

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
  
  console.log(`📤 Sending: ${method}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Request ${method} timed out`));
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
              responseBuffer = lines[lines.length - 1];
              resolve(response);
              return;
            }
          } catch (error) {
            // Ignore parse errors
          }
        }
      }
      responseBuffer = lines[lines.length - 1];
    }
    
    mcpServer.stdout.on('data', processResponse);
  });
}

async function exploreWorkspace() {
  try {
    // List all items in workspace
    console.log('\n📋 Step 1: Listing ALL items in workspace...');
    const allItemsResponse = await sendMcpRequest('tools/call', {
      name: 'list-fabric-items',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        itemType: 'All'
      }
    });
    
    console.log('✅ All items response:');
    console.log(JSON.stringify(allItemsResponse, null, 2));
    
    // Try creating a notebook instead (usually works)
    console.log('\n📓 Step 2: Creating a notebook...');
    const notebookResponse = await sendMcpRequest('tools/call', {
      name: 'create-fabric-notebook',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        displayName: 'MCP Test Notebook',
        description: 'Test notebook created via MCP server'
      }
    });
    
    console.log('✅ Notebook creation response:');
    console.log(JSON.stringify(notebookResponse, null, 2));
    
    // Check different item types
    const itemTypes = ['Notebook', 'Dataset', 'Report', 'Dashboard'];
    for (const itemType of itemTypes) {
      console.log(`\n🔍 Step 3.${itemTypes.indexOf(itemType) + 1}: Listing ${itemType} items...`);
      try {
        const response = await sendMcpRequest('tools/call', {
          name: 'list-fabric-items',
          arguments: {
            bearerToken: config.bearerToken,
            workspaceId: config.workspaceId,
            itemType: itemType
          }
        });
        
        console.log(`✅ ${itemType} items:`, response.result?.content?.[0]?.text || 'No response');
      } catch (error) {
        console.log(`❌ Error listing ${itemType}:`, error.message);
      }
    }
    
  } catch (error) {
    console.error('❌ Error exploring workspace:', error.message);
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
    setTimeout(exploreWorkspace, 2000);
  }
});

mcpServer.stdout.on('data', (data) => {
  const message = data.toString();
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
