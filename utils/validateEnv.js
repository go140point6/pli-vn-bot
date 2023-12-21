var validateEnv = function () {
    if (!process.env.BOT_TOKEN) {
        console.warn("Missing Discord bot token.");
        process.exit(1);
    }
};

exports.validateEnv = validateEnv;