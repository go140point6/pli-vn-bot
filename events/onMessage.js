// Example of a Bot saying "Bingo" to any message from a user (ignoring itself)
async function onMessage(message) {
    //if(message.author.bot) return;

    //if(message.author.bot) {
    //    console.log(message);
    //}
    
    //message.reply("Bingo")
    //console.log(message);

};

module.exports = { 
    onMessage
}