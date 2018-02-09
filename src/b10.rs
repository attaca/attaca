const SMALLEST_INTEGER_WITH_N_DIGITS: [u64; 20] = [
    0,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
];

const GUESS: [u32; 65] = [
    0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9,
    9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15, 15, 16, 16,
    16, 17, 17, 17, 18, 18, 18, 18, 19,
];

/// Find the number of decimal digits needed to display a `u64`. Aside from the case of `0`, this
/// is an integer logarithm, rounded down. `b10_digits(0) == 1` because if a number is `0`, one
/// digit - '0' itself - is still needed to display it.
pub fn b10_digits(n: u64) -> u32 {
    let guess = GUESS[(64 - n.leading_zeros()) as usize];
    if n >= SMALLEST_INTEGER_WITH_N_DIGITS[guess as usize] {
        guess + 1
    } else {
        guess
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b10_digits_simple(mut n: u64) -> u32 {
        let mut d = 0;

        while n > 0 {
            n /= 10;
            d += 1;
        }

        if d == 0 {
            1
        } else {
            d
        }
    }

    quickcheck! {
        fn same_as_simple(n: u64) -> bool {
            b10_digits(n) == b10_digits_simple(n)
        }
    }
}
