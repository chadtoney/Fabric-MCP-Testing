const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    sessionId: "23768e94-7991-45ac-b1c4-ed8b89d7f98c",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
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

async function checkExecutionResults() {
    console.log('🔍 Checking Livy Statement Execution Results');
    console.log('='.repeat(50));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        // Check status of the first statement (test)
        console.log('📋 Checking first statement (test code)...');
        const statement1Result = await sendMCPRequest('get-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: config.sessionId,
            statementId: 1
        });
        
        console.log('Statement 1 Result:');
        console.log('='.repeat(30));
        const statement1Data = JSON.parse(statement1Result.content[0].text);
        console.log(`State: ${statement1Data.state}`);
        if (statement1Data.output) {
            console.log('Output:');
            console.log(statement1Data.output.data?.['text/plain'] || 'No output');
        }
        console.log('='.repeat(30));
        
        // Check status of the second statement (data population)
        console.log('\n📋 Checking second statement (data population)...');
        const statement2Result = await sendMCPRequest('get-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: config.sessionId,
            statementId: 2
        });
        
        console.log('Statement 2 Result:');
        console.log('='.repeat(30));
        const statement2Data = JSON.parse(statement2Result.content[0].text);
        console.log(`State: ${statement2Data.state}`);
        if (statement2Data.output) {
            console.log('Output:');
            console.log(statement2Data.output.data?.['text/plain'] || 'No output');
        }
        console.log('='.repeat(30));
        
        // If statements are still running, wait and check again
        if (statement2Data.state === 'running' || statement2Data.state === 'waiting') {
            console.log('\n⏳ Statements still executing, waiting 30 more seconds...');
            await new Promise(resolve => setTimeout(resolve, 30000));
            
            const finalCheck = await sendMCPRequest('get-livy-statement', {
                bearerToken: token,
                workspaceId: config.workspaceId,
                lakehouseId: config.lakehouseId,
                sessionId: config.sessionId,
                statementId: 2
            });
            
            console.log('\n📋 Final Check of Data Population:');
            console.log('='.repeat(40));
            const finalData = JSON.parse(finalCheck.content[0].text);
            console.log(`State: ${finalData.state}`);
            if (finalData.output) {
                console.log('Output:');
                console.log(finalData.output.data?.['text/plain'] || 'No output');
            }
            console.log('='.repeat(40));
        }
        
        // Now let's verify the data was actually created
        console.log('\n🔍 Verifying data in lakehouse...');
        const verificationCode = `
# Verify the data population worked
print("🔍 Verification Report")
print("=" * 40)

# Check available tables
print("📊 Available Tables:")
tables = spark.sql("SHOW TABLES")
tables.show()

# Check customers table
print("\\n👥 Customers Table:")
try:
    customer_count = spark.sql("SELECT COUNT(*) as count FROM customers").collect()[0]['count']
    print(f"  Total customers: {customer_count:,}")
    
    # Show sample data
    print("\\n📋 Sample customer data:")
    spark.sql("SELECT * FROM customers LIMIT 5").show()
    
    # Show customer statistics
    print("\\n📈 Customer statistics:")
    spark.sql("""
        SELECT 
            COUNT(*) as total_customers,
            AVG(customer_value) as avg_value,
            MIN(customer_value) as min_value,
            MAX(customer_value) as max_value,
            COUNT(CASE WHEN is_active = true THEN 1 END) as active_customers
        FROM customers
    """).show()
except Exception as e:
    print(f"❌ Error checking customers: {e}")

# Check products table
print("\\n🛍️ Products Table:")
try:
    product_count = spark.sql("SELECT COUNT(*) as count FROM products").collect()[0]['count']
    print(f"  Total products: {product_count:,}")
    
    # Show sample data
    print("\\n📋 Sample product data:")
    spark.sql("SELECT * FROM products LIMIT 5").show()
    
    # Show product statistics
    print("\\n📈 Product statistics:")
    spark.sql("""
        SELECT 
            category,
            COUNT(*) as product_count,
            AVG(price) as avg_price,
            AVG(stock_quantity) as avg_stock
        FROM products 
        GROUP BY category 
        ORDER BY product_count DESC
    """).show()
except Exception as e:
    print(f"❌ Error checking products: {e}")

print("\\n🎉 Verification completed!")
        `;
        
        const verificationResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: config.sessionId,
            code: verificationCode,
            kind: 'pyspark'
        });
        
        console.log('\n📊 Verification Statement Created:');
        console.log(JSON.stringify(JSON.parse(verificationResult.content[0].text), null, 2));
        
        // Wait and get verification results
        console.log('\n⏳ Waiting 20 seconds for verification to complete...');
        await new Promise(resolve => setTimeout(resolve, 20000));
        
        const verificationCheck = await sendMCPRequest('get-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: config.sessionId,
            statementId: 3
        });
        
        console.log('\n📋 FINAL VERIFICATION RESULTS:');
        console.log('='.repeat(50));
        const verificationData = JSON.parse(verificationCheck.content[0].text);
        console.log(`State: ${verificationData.state}`);
        if (verificationData.output) {
            console.log('Output:');
            console.log(verificationData.output.data?.['text/plain'] || 'No output');
        }
        console.log('='.repeat(50));
        
    } catch (error) {
        console.error('❌ Error checking results:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

checkExecutionResults().catch(console.error);
