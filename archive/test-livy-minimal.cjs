const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61',
  existingSessionId: 72 // Use session 72 which is the most recent reliable-session
};

console.log('📊 Testing Livy Execution with Minimal Sample Data');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);
console.log(`Using session ID: ${config.existingSessionId}`);

// Very simple test code
const testCode = `
print("🔥 Testing Livy execution...")

# Simple test to create a basic table
from pyspark.sql import Row

# Create minimal test data
test_data = [
    Row(id=1, name="Test Product 1", price=100.0),
    Row(id=2, name="Test Product 2", price=200.0),
    Row(id=3, name="Test Product 3", price=300.0)
]

# Create DataFrame
df = spark.createDataFrame(test_data)
print(f"✅ Created test DataFrame with {df.count()} rows")

# Show the data
print("📋 Test data:")
df.show()

# Try to save as a table
try:
    df.write.format("delta").mode("overwrite").saveAsTable("test_table")
    print("✅ SUCCESS: Saved test_table to lakehouse!")
except Exception as e:
    print(f"❌ Error saving table: {e}")

print("🎉 Test execution completed!")
`;

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
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Request ${method} timed out after 45 seconds`));
    }, 45000);
    
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
            // Ignore JSON parse errors for incomplete lines
          }
        }
      }
      responseBuffer = lines[lines.length - 1];
    }
    
    mcpServer.stdout.on('data', processResponse);
  });
}

async function testLivyExecution() {
  try {
    console.log('\n📊 Step 1: Testing minimal code execution...');
    const dataResponse = await sendMcpRequest('tools/call', {
      name: 'execute-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: config.existingSessionId,
        code: testCode,
        kind: 'pyspark'
      }
    });
    
    console.log('✅ Execution response received');
    
    if (dataResponse.error) {
      console.error('❌ Execution error:', dataResponse.error.message);
      return;
    }
    
    const execContent = dataResponse.result.content[0].text;
    console.log('📋 Raw response:', execContent);
    
    let statementId;
    try {
      const execData = JSON.parse(execContent);
      statementId = execData.id;
      console.log(`🎯 Statement ID: ${statementId}, Status: ${execData.state}`);
    } catch (parseError) {
      console.log('❌ Could not parse execution response');
      console.log('This might mean Livy execution is having issues');
      return;
    }
    
    // Wait for execution
    console.log('\n⏳ Waiting 30 seconds for execution...');
    await new Promise(resolve => setTimeout(resolve, 30000));
    
    console.log('\n🔍 Step 2: Checking results...');
    const resultResponse = await sendMcpRequest('tools/call', {
      name: 'get-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: config.existingSessionId,
        statementId: statementId
      }
    });
    
    if (resultResponse.error) {
      console.log('❌ Error getting results:', resultResponse.error.message);
    } else {
      const resultContent = resultResponse.result.content[0].text;
      console.log('✅ Result response:', resultContent);
      
      try {
        const resultData = JSON.parse(resultContent);
        console.log(`📊 Status: ${resultData.state}`);
        
        if (resultData.output && resultData.output.data) {
          console.log('📋 Output:');
          console.log(resultData.output.data['text/plain']);
          
          if (resultData.output.data['text/plain'].includes('SUCCESS')) {
            console.log('🎉 EXCELLENT! Livy execution is working!');
            console.log('✅ We can proceed with full sample data generation');
          }
        }
      } catch (parseError) {
        console.log('Raw result:', resultContent);
      }
    }
    
  } catch (error) {
    console.error('❌ Error in test execution:', error.message);
  } finally {
    console.log('\n🛑 Stopping MCP server...');
    mcpServer.kill();
    setTimeout(() => process.exit(0), 2000);
  }
}

// Handle MCP server startup
mcpServer.stderr.on('data', (data) => {
  const message = data.toString();
  console.log('🔧 MCP Server:', message.trim());
  
  if (message.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server started successfully');
    setTimeout(testLivyExecution, 2000);
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
