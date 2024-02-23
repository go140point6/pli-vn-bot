// Function to calculate remaining time
async function getRemainingTime() {
    const elapsedTime = Date.now() - startTime;
    const remainingTime = modalTimeout - elapsedTime;
    return Math.max(0, remainingTime); // Ensure remaining time is not negative
}

// Function to report remaining time
async function reportRemainingTime() {
    const remainingTime = getRemainingTime();
    const secondsLeft = Math.ceil(remainingTime / 1000);
    return `Time left: ${secondsLeft} seconds`;
}

module.exports = {
    getRemainingTime,
    reportRemainingTime
}