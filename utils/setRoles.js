async function clearRoles(guild, member) {
    const red = await guild.roles.cache.find(role => role.name === 'tickers-red')
    const green = await guild.roles.cache.find(role => role.name === 'tickers-green')
    await member.roles.remove(red)
    await member.roles.remove(green)
}
  
async function setRed(guild, member) {
    const red = await guild.roles.cache.find(role => role.name === 'tickers-red')
    console.log('Setting Red Role Now...')
    await clearRoles(guild, member)
    await member.roles.add(red)
    let redRole = await member.roles.cache.some(role => role.name === ('tickers-red'))
    console.log ('Attempted adding of redRole, if successful, this should be true:', redRole)
    if (!redRole) {
        console.log ('ERROR, still showing false for redRole... trying again...')
        await (member.roles.add(red))
        let redRole = await member.roles.cache.some(role => role.name === ('tickers-red'))
        console.log ('Attempted 2nd adding of redRole, if successful, this should be true:', redRole)
    }
}
  
async function setGreen(guild, member) {
    const green = await guild.roles.cache.find(role => role.name === 'tickers-green')
    console.log('Setting Green Role Now...')
    await clearRoles(guild, member)
    await member.roles.add(green)
    let greenRole = await member.roles.cache.some(role => role.name === ('tickers-green'))
    console.log ('Attempted adding of greenRole, if successful, this should be true:', greenRole)
    if (!greenRole) {
       console.log ('ERROR, still showing false for greenRole... trying again...')
       await (member.roles.add(green))
       let greenRole = await member.roles.cache.some(role => role.name === ('tickers-green'))
       console.log ('Attempted 2nd adding of greenRole, if successful, this should be true:', greenRole)
    }
}

module.exports = {
    clearRoles,
    setRed,
    setGreen
}