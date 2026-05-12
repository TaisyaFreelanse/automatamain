use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[derive(deluxe::ExtractAttributes)]
#[deluxe(attributes(ix_data))]
struct InstructionData {
    discriminator: Vec<u8>,
    accounts: Option<syn::Type>,
}

fn derive_instruction2(
    input: proc_macro2::TokenStream,
) -> deluxe::Result<proc_macro2::TokenStream> {
    let mut ast: DeriveInput = syn::parse2(input)?;

    let ix_data: InstructionData = deluxe::extract_attributes(&mut ast)?;

    let accounts = ix_data.accounts;
    let discriminator = ix_data.discriminator;

    let name = ast.ident;

    let bytes = discriminator.iter();

    let disc = quote! {
        impl InstructionArgs for #name {
            const DISCRIMINATOR: &'static [u8] = &[#(#bytes),*];
        }
    };

    Ok(quote! {
        #disc
    })
}

#[proc_macro_derive(Instruction, attributes(ix_data))]
pub fn derive_instruction(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive_instruction2(input.into())
        .unwrap_or_else(|e| e.into_compile_error())
        .into()
}

#[proc_macro_derive(Instructions, attributes(program))]
pub fn derive_instruction_set(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // 1. Extract the Program ID from attributes
    let program_attr = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("program"))
        .expect("enum must have #[program(\"...\")] attribute");

    let program_lit: syn::LitStr = program_attr
        .parse_args()
        .expect("program attribute must be a string");

    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => panic!("Instructions can only be derived for enums"),
    };

    // 2. Generate Trait Implementations for each inner Instruction struct
    // and build the match arms for the From conversion
    let mut trait_impls = Vec::new();
    let mut match_arms = Vec::new();

    for variant in variants {
        let v_ident = &variant.ident;

        // Ensure it's a tuple variant with exactly one field: Variant(InnerType)
        let inner_type = match &variant.fields {
            Fields::Unnamed(f) if f.unnamed.len() == 1 => &f.unnamed[0].ty,
            _ => panic!(
                "Variant {} must be a tuple variant with 1 inner type",
                v_ident
            ),
        };

        // Implementation of ProgramAddress for the inner struct
        trait_impls.push(quote! {
            impl ProgramAddress for #inner_type {
                fn program(&self) -> &'static Address {
                    &#name::PROGRAM
                }
            }
        });

        // Arm for the From<Enum> -> RawInstruction conversion
        match_arms.push(quote! {
            #name::#v_ident(ix) => ix.into_raw(#name::PROGRAM)
        });
    }

    // 3. Construct the final output
    let expanded = quote! {
        // Shared constant for the Program ID
        impl #name {
            pub const PROGRAM: Address = Address(five8_const::decode_32_const(#program_lit));
        }

        // Implement ProgramAddress for every specific instruction struct
        #(#trait_impls)*

        // Replace .raw() with a standard From conversion
        impl From<#name> for RawInstruction {
            fn from(ix_set: #name) -> Self {
                match ix_set {
                    #(#match_arms),*
                }
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Accounts, attributes(signer, writable))]
pub fn derive_accounts(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => panic!("Accounts derive can only be used on structs with named fields"),
        },
        _ => panic!("Accounts derive can only be used on structs"),
    };

    // collect the identifiers and signer/writable flags
    let meta_entries = fields.iter().map(|f| {
        let field_ident = &f.ident;
        let is_signer = f.attrs.iter().any(|attr| attr.path().is_ident("signer"));
        let is_writable = f.attrs.iter().any(|attr| attr.path().is_ident("writable"));
        quote! {
            v.push(AccountMeta {
                pubkey: self.#field_ident,
                is_signer: #is_signer,
                writable: #is_writable,
            });
        }
    });

    let count = fields.len();

    let expanded = quote! {
        impl IntoAccountMetaArray for #name {
            fn accounts_meta(self) -> alloc::vec::Vec<AccountMeta> {
                let mut v = alloc::vec::Vec::with_capacity(#count);
                #(#meta_entries)*
                v
            }
        }
    };

    TokenStream::from(expanded)
}
