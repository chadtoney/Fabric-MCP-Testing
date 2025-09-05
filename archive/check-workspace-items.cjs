const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli'
};

console.log('🔍 Listing available items in the workspace...');
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
  
  console.log(`📤 Sending: ${JSON.stringify(request)}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Request timeout'));
    }, 30000);
    
    function handleResponse(data) {
      responseBuffer += data;
      
      const lines = responseBuffer.split('\n');
      responseBuffer = lines.pop() || '';
      
      for (const line of lines) {
        if (!line.trim()) continue;
        
        try {
          const response = JSON.parse(line);
          if (response.id === request.id) {
            clearTimeout(timeout);
            mcpServer.stdout.removeListener('data', handleResponse);
            console.log(`📥 Received: ${JSON.stringify(response, null, 2)}`);
            resolve(response);
            return;
          }
        } catch (err) {
          // Ignore parsing errors for intermediate messages
        }
      }
    }
    
    mcpServer.stdout.on('data', handleResponse);
  });
}

async function checkWorkspaceItems() {
  try {
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log('\n🔍 Step 1: Listing workspace items...');
    const itemsResponse = await sendMcpRequest('tools/call', {
      name: 'list-fabric-items',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId
      }
    });
    
    console.log('\n📋 Workspace items result:');
    console.log(JSON.stringify(itemsResponse, null, 2));
    
    // Also check if there are specific lakehouse listing tools
    console.log('\n🔍 Step 2: Checking for lakehouses specifically...');
    const lakehouseResponse = await sendMcpRequest('tools/call', {
      name: 'list-fabric-lakehouses',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId
      }
    });
    
    console.log('\n🏞️ Lakehouses result:');
    console.log(JSON.stringify(lakehouseResponse, null, 2));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    mcpServer.kill();
    console.log('\n🏁 Process completed');
  }
}

// Handle server errors
mcpServer.stderr.on('data', (data) => {
  const output = data.toString();
  if (output.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server is ready');
  } else if (output.includes('error') || output.includes('Error')) {
    console.error('Server error:', output);
  }
});

mcpServer.on('close', (code) => {
  console.log(`Server process exited with code ${code}`);
});

// Start the process
checkWorkspaceItems();
