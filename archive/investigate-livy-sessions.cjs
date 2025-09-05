const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61', // MCPDemoLakehouse ID (correct ID)
  bearerToken: 'azure_cli'
};

console.log('🔍 Investigating Livy session management...');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

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
  
  console.log(`📤 Sending: ${JSON.stringify(request, null, 100)}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Request timeout'));
    }, 60000);
    
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

async function investigateLivySessions() {
  try {
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log('\n🔍 Step 1: Listing existing Livy sessions...');
    const listResponse = await sendMcpRequest('tools/call', {
      name: 'list-livy-sessions',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId
      }
    });
    
    console.log('\n📋 Existing sessions:');
    console.log(JSON.stringify(listResponse, null, 2));
    
    console.log('\n🚀 Step 2: Creating a new Livy session...');
    const createResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark'
        }
      }
    });
    
    console.log('\n📋 Create session response:');
    console.log(JSON.stringify(createResponse, null, 2));
    
    // Now list sessions again to see the structure
    console.log('\n🔍 Step 3: Listing sessions after creation...');
    const listAfterResponse = await sendMcpRequest('tools/call', {
      name: 'list-livy-sessions',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId
      }
    });
    
    console.log('\n📋 Sessions after creation:');
    console.log(JSON.stringify(listAfterResponse, null, 2));
    
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
investigateLivySessions();
