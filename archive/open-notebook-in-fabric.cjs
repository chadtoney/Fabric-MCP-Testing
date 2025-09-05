const { execSync } = require('child_process');

function openNotebookInFabric() {
    console.log('🚀 Opening notebook in Fabric workspace...');
    
    const workspaceId = 'bcb44215-0e69-46d3-aac9-fb92fadcd982';
    const notebookId = '1df1becd-d296-4212-9e20-dceb390d3994';
    
    // Direct URL to the notebook in Fabric
    const notebookUrl = `https://app.fabric.microsoft.com/groups/${workspaceId}/synapsenotebooks/${notebookId}`;
    
    console.log('📋 Notebook Details:');
    console.log(`   - Workspace ID: ${workspaceId}`);
    console.log(`   - Notebook ID: ${notebookId}`);
    console.log(`   - Notebook Name: LakehouseDataPopulation`);
    console.log('');
    console.log('🌐 Opening notebook in browser...');
    console.log(`   URL: ${notebookUrl}`);
    
    try {
        // Open the notebook in the default browser
        if (process.platform === 'win32') {
            execSync(`start "" "${notebookUrl}"`, { stdio: 'ignore' });
        } else if (process.platform === 'darwin') {
            execSync(`open "${notebookUrl}"`, { stdio: 'ignore' });
        } else {
            execSync(`xdg-open "${notebookUrl}"`, { stdio: 'ignore' });
        }
        
        console.log('✅ Browser opened successfully!');
    } catch (error) {
        console.log('⚠️ Could not open browser automatically.');
        console.log('Please copy and paste the URL above into your browser.');
    }
    
    console.log('');
    console.log('📋 Steps to run the notebook in Fabric:');
    console.log('1. ✅ Browser should open to the notebook automatically');
    console.log('2. 🏠 Ensure "MCPDemoLakehouse" is set as the default lakehouse');
    console.log('3. ▶️ Click "Run all" to execute all cells');
    console.log('4. ⏱️ Wait for execution to complete (~2-3 minutes)');
    console.log('5. 📊 Review the generated data and analytics tables');
    console.log('');
    console.log('💡 Expected output:');
    console.log('   - demo_sales table (1,000 records)');
    console.log('   - product_performance_summary table');
    console.log('   - regional_sales_analysis table');
    console.log('   - monthly_sales_trends table');
    console.log('   - sales_channel_performance table');
    console.log('   - customer_insights table');
    console.log('');
    console.log('🎉 Your lakehouse will be populated with comprehensive analytics data!');
}

// Run the function
openNotebookInFabric();
