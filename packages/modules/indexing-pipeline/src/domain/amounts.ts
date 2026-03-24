import { ValidationError } from '@onlydoge/shared-kernel';

export function parseAmountBase(value: string): bigint {
  const trimmed = value.trim();
  if (!/^-?\d+$/u.test(trimmed)) {
    throw new ValidationError(`invalid amount base: ${value}`);
  }

  return BigInt(trimmed);
}

export function formatAmountBase(value: bigint): string {
  return value.toString();
}

export function addAmountBase(left: string, right: string): string {
  return formatAmountBase(parseAmountBase(left) + parseAmountBase(right));
}

export function subtractAmountBase(left: string, right: string): string {
  return formatAmountBase(parseAmountBase(left) - parseAmountBase(right));
}

export function fromDecimalUnits(value: number | string, decimals: number): string {
  const source = typeof value === 'number' ? value.toFixed(decimals) : value.trim();
  const match = source.match(/^(-?)(\d+)(?:\.(\d+))?$/u);
  if (!match) {
    throw new ValidationError(`invalid decimal amount: ${value}`);
  }

  const [, sign, whole, fraction = ''] = match;
  if (fraction.length > decimals && /[^0]/u.test(fraction.slice(decimals))) {
    throw new ValidationError(`invalid decimal amount: ${value}`);
  }

  const paddedFraction = fraction.slice(0, decimals).padEnd(decimals, '0');
  const normalized = `${whole}${paddedFraction}`.replace(/^0+(?=\d)/u, '') || '0';
  return `${sign}${normalized}`;
}

export function fromHexUnits(value: string): string {
  const normalized = value.trim().startsWith('0x') ? value.trim() : `0x${value.trim()}`;
  return formatAmountBase(BigInt(normalized));
}
