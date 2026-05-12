use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::ops::{Add, Div, Sub};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
pub struct Amount {
    value: u64,
    decimals: u8,
}

impl Amount {
    pub const fn new(value: u64, decimals: u8) -> Self {
        Self { value, decimals }
    }

    pub const fn from_float(v: f64, decimals: u8) -> Self {
        let scale = 10u64.pow(decimals as u32);
        Self {
            value: (v * scale as f64).round() as u64,
            decimals,
        }
    }
    pub const fn from_float_native(v: f64) -> Self {
        let decimals = 9;
        let scale = 10u64.pow(decimals as u32);
        Self {
            value: (v * scale as f64).round() as u64,
            decimals,
        }
    }

    pub const fn from_raw(value: u64, decimals: u8) -> Self {
        Self { value, decimals }
    }

    pub const fn from_raw_native(value: u64) -> Self {
        Self { value, decimals: 9 }
    }

    pub fn decimals(&self) -> u8 {
        self.decimals
    }

    pub fn raw(&self) -> u64 {
        self.value
    }

    pub fn to_float(&self) -> f64 {
        self.value as f64 / 10u64.pow(self.decimals as u32) as f64
    }

    pub fn to_scale(&self, new_decimals: u8) -> Self {
        if new_decimals > self.decimals {
            let factor = 10u64.pow((new_decimals - self.decimals) as u32);
            Self {
                value: self.value * factor,
                decimals: new_decimals,
            }
        } else {
            let factor = 10u64.pow((self.decimals - new_decimals) as u32);
            Self {
                value: self.value / factor,
                decimals: new_decimals,
            }
        }
    }
}

// arithmetic: only allow same decimal arithmetic
impl Add for Amount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(self.decimals, rhs.decimals, "decimal mismatch");
        Self {
            value: self.value + rhs.value,
            decimals: self.decimals,
        }
    }
}

impl Sub for Amount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(self.decimals, rhs.decimals, "decimal mismatch");
        Self {
            value: self.value - rhs.value,
            decimals: self.decimals,
        }
    }
}

impl Div for Amount {
    type Output = f64;

    fn div(self, rhs: Self) -> Self::Output {
        self.to_float() / rhs.to_float()
    }
}

impl core::fmt::Display for Amount {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let scale = 10u64.pow(self.decimals as u32);
        let int_part = self.value / scale;
        let frac_part = self.value % scale;

        if self.decimals == 0 {
            write!(f, "{}", int_part)
        } else {
            write!(
                f,
                "{}.{:0width$}",
                int_part,
                frac_part,
                width = self.decimals as usize
            )
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
pub struct SignedAmount {
    value: i64,
    decimals: u8,
}

impl SignedAmount {
    pub const fn new(value: i64, decimals: u8) -> Self {
        Self { value, decimals }
    }

    pub const fn from_float(v: f64, decimals: u8) -> Self {
        let scale = 10i64.pow(decimals as u32);
        Self {
            value: (v * scale as f64).round() as i64,
            decimals,
        }
    }
    pub const fn from_float_native(v: f64) -> Self {
        let decimals = 9;
        let scale = 10u64.pow(decimals as u32);
        Self {
            value: (v * scale as f64).round() as i64,
            decimals,
        }
    }

    pub const fn from_raw(value: i64, decimals: u8) -> Self {
        Self { value, decimals }
    }

    pub const fn from_raw_native(value: i64) -> Self {
        Self { value, decimals: 9 }
    }

    pub fn decimals(&self) -> u8 {
        self.decimals
    }

    pub fn raw(&self) -> i64 {
        self.value
    }

    pub fn to_float(&self) -> f64 {
        self.value as f64 / 10u64.pow(self.decimals as u32) as f64
    }

    pub fn to_scale(&self, new_decimals: u8) -> Self {
        if new_decimals > self.decimals {
            let factor = 10i64.pow((new_decimals - self.decimals) as u32);
            Self {
                value: self.value * factor,
                decimals: new_decimals,
            }
        } else {
            let factor = 10i64.pow((self.decimals - new_decimals) as u32);
            Self {
                value: self.value / factor,
                decimals: new_decimals,
            }
        }
    }
}

// arithmetic: only allow same decimal arithmetic
impl Add for SignedAmount {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(self.decimals, rhs.decimals, "decimal mismatch");
        Self {
            value: self.value + rhs.value,
            decimals: self.decimals,
        }
    }
}

impl Sub for SignedAmount {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(self.decimals, rhs.decimals, "decimal mismatch");
        Self {
            value: self.value - rhs.value,
            decimals: self.decimals,
        }
    }
}

impl Div for SignedAmount {
    type Output = f64;

    fn div(self, rhs: Self) -> Self::Output {
        self.to_float() / rhs.to_float()
    }
}

impl core::fmt::Display for SignedAmount {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let scale = 10u64.pow(self.decimals as u32);
        let abs_value = self.value.unsigned_abs();
        let int_part = abs_value / scale;
        let frac_part = abs_value % scale;

        if self.value < 0 {
            if self.decimals == 0 {
                write!(f, "-{}", int_part)
            } else {
                write!(
                    f,
                    "-{}. {:0width$}",
                    int_part,
                    frac_part,
                    width = self.decimals as usize
                )
            }
        } else if self.decimals == 0 {
            write!(f, "{}", int_part)
        } else {
            write!(
                f,
                "{}.{:0width$}",
                int_part,
                frac_part,
                width = self.decimals as usize
            )
        }
    }
}
