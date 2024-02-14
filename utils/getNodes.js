async function getNodes() {
    let activeNodes = []
    try {
        const res = await fetch('https://oracles.goplugin.co/api/nodesetting/activenodelist').then(response => response.json())
        const table = res.table
        //console.log(res)

        const fromDate = new Date('2023-12-27')
        activeNodes = table.filter(node => new Date(node.date) >= fromDate)

        const nodeCount = activeNodes.length
        //console.log("Active Nodes from 12/27/2023 onward:", activeNodes)
        //console.log("Number of active nodes from 12/27/2023 onward:", nodeCount); // Log the nodeCount for debugging
        return nodeCount
    } catch (error) {
        console.error("Error fetching or processing nodes:", error)
        return 0
    }
}
        //let nodes = res.dashboard.node.toString()
        //let inactive = res.dashboard.inactive.toString()
        //console.log("Current nodes: " + nodes);
        //console.log("Inactive nodes: " + inactive)
        //return { nodes, inactive }

module.exports = {
    getNodes,
}