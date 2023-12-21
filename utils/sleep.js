const sleep = async (ms) => await new Promise(r => setTimeout(r,ms));

exports.sleep = sleep;