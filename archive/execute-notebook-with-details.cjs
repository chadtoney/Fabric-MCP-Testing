const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  notebookId: '1df1becd-d296-4212-9e20-dceb390d3994',
  bearerToken: 'azure_cli'
};

console.log('🚀 Executing notebook using execute-fabric-notebook tool...');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Notebook: ${config.notebookId}`);

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
    }, 60000); // Increased timeout for notebook execution
    
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

async function executeNotebook() {
  try {
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log('\n🚀 Step 1: Executing notebook with execute-fabric-notebook...');
    const executeResponse = await sendMcpRequest('tools/call', {
      name: 'execute-fabric-notebook',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        notebookId: config.notebookId,
        parameters: {}
      }
    });
    
    console.log('\n✅ Notebook execution response:');
    console.log(JSON.stringify(executeResponse, null, 2));
    
    // If that didn't work, let's also try checking the notebook details first
    if (executeResponse.error || (executeResponse.result && executeResponse.result.content[0].text.includes('Error'))) {
      console.log('\n🔍 Step 2: Getting notebook details for troubleshooting...');
      
      const detailsResponse = await sendMcpRequest('tools/call', {
        name: 'get-fabric-notebook',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          notebookId: config.notebookId
        }
      });
      
      console.log('\n📋 Notebook details:');
      console.log(JSON.stringify(detailsResponse, null, 2));
    }
    
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
executeNotebook();
