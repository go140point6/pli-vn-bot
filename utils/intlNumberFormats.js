const createCurrencyFormatter = (currency, minFractionDigits, maxFractionDigits) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: currency,
      minimumFractionDigits: minFractionDigits,
      maximumFractionDigits: maxFractionDigits,
    });
  };

const createDecimalFormatter = (minFractionDigits, maxFractionDigits) => {
    return new Intl.NumberFormat('en-US', {
      style: 'decimal',
      minimumFractionDigits: minFractionDigits,
      maximumFractionDigits: maxFractionDigits,
    });
  };

  const createPercentFormatter = (minFractionDigits, maxFractionDigits) => {
    return new Intl.NumberFormat('en-US', {
      style: 'percent',
      minimumFractionDigits: minFractionDigits,
      maximumFractionDigits: maxFractionDigits,
    });
  };

module.exports = {
    createCurrencyFormatter,
    createDecimalFormatter,
    createPercentFormatter
}