const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

let mcpProcess;

async function sendMCPRequest(tool, params) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error(`Timeout for ${tool}`));
        }, 60000);

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

        console.log(`📤 Calling ${tool}...`);
        
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
                                console.log(`❌ Error response:`, parsed.error);
                                reject(new Error(`${parsed.error.code}: ${parsed.error.message}`));
                            } else {
                                console.log(`✅ Success for ${tool}`);
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

async function useExistingSession() {
    console.log('🔄 Using Existing Session for Sample Data');
    console.log(`Workspace: ${config.workspaceId}`);
    console.log(`Lakehouse: ${config.lakehouseId}\n`);
    
    mcpProcess = spawn('node', ['build/index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: process.cwd()
    });

    // Wait for startup
    console.log('⏳ Starting MCP server...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    console.log('✅ MCP server ready\n');

    try {
        // Get list of sessions
        console.log('📋 Getting list of Livy sessions...');
        const sessions = await sendMCPRequest('list-livy-sessions', {
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId
        });
        
        console.log(`Found ${sessions.sessions?.length || 0} sessions:`);
        sessions.sessions?.forEach((session, index) => {
            console.log(`  ${index + 1}. Session ${session.id}: ${session.state} (${session.kind})`);
        });

        // Use the first idle session
        const idleSession = sessions.sessions?.find(s => s.state === 'idle');
        if (!idleSession) {
            console.log('❌ No idle sessions available');
            return;
        }

        console.log(`\n🎯 Using session ${idleSession.id} for sample data creation`);

        // Sample data creation code
        const sampleDataCode = `
# Create sample sales data
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

print("Creating sample sales data...")

# Sample data generation
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
regions = ["North", "South", "East", "West", "Central"]

# Create sample sales records
sales_data = []
start_date = datetime(2024, 1, 1)

for i in range(50):
    sale_date = start_date + timedelta(days=random.randint(0, 365))
    sales_data.append({
        "sale_id": i + 1,
        "product": random.choice(products),
        "region": random.choice(regions),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(10.0, 500.0), 2),
        "sale_date": sale_date.strftime("%Y-%m-%d")
    })

# Create DataFrame
sales_df = spark.createDataFrame(sales_data)

# Add calculated columns
sales_df = sales_df.withColumn("total_amount", col("quantity") * col("unit_price"))

print(f"Created {sales_df.count()} sales records")
sales_df.show(10)

# Save to table
sales_df.write.mode("overwrite").saveAsTable("demo_sales")
print("✅ Saved sales data to demo_sales table")

# Create product summary
product_summary = sales_df.groupBy("product").agg(
    sum("quantity").alias("total_quantity"),
    sum("total_amount").alias("total_revenue"),
    count("*").alias("sale_count"),
    avg("unit_price").alias("avg_unit_price")
).orderBy(desc("total_revenue"))

print("Product Summary:")
product_summary.show()

# Save summary table
product_summary.write.mode("overwrite").saveAsTable("product_summary")
print("✅ Saved product summary to product_summary table")

print("🎉 Sample data creation completed successfully!")
        `;

        console.log('\n🚀 Executing sample data creation...');
        const statement = await sendMCPRequest('execute-livy-statement', {
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: idleSession.id,
            code: sampleDataCode.trim(),
            kind: 'pyspark'
        });
        
        console.log(`📝 Statement ${statement.id} submitted`);
        
        // Wait for execution
        console.log('⏳ Waiting for execution to complete...');
        let attempts = 0;
        const maxAttempts = 12; // 2 minutes
        
        while (attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 10000)); // Wait 10 seconds
            attempts++;
            
            try {
                const result = await sendMCPRequest('get-livy-statement', {
                    workspaceId: config.workspaceId,
                    lakehouseId: config.lakehouseId,
                    sessionId: idleSession.id,
                    statementId: statement.id
                });
                
                console.log(`📊 Statement state: ${result.state} (attempt ${attempts})`);
                
                if (result.state === 'available') {
                    console.log('\n🎉 Execution completed successfully!');
                    if (result.output) {
                        console.log('📄 Output:');
                        console.log(result.output.data?.['text/plain'] || JSON.stringify(result.output, null, 2));
                    }
                    break;
                } else if (result.state === 'error') {
                    console.log('❌ Execution failed');
                    if (result.output) {
                        console.log('Error details:', result.output);
                    }
                    break;
                }
            } catch (error) {
                console.log(`⚠️ Error checking status: ${error.message}`);
            }
        }
        
        if (attempts >= maxAttempts) {
            console.log('⏰ Execution timeout - please check Fabric portal for results');
        }

    } catch (error) {
        console.error(`❌ Error: ${error.message}`);
    }

    console.log('\n🛑 Stopping MCP server...');
    mcpProcess.kill();
}

useExistingSession().catch(console.error);
