const sleep = async (ms) => new Promise(r => setTimeout(r,ms));

exports.sleep = sleep

module.exports = {
    sleep
}