//! The `#[computation]` proc-macro: generates everything Phase A's
//! `computations::flow` module was hand-written to accept (see that
//! module's docs, and `computations/tests/flow.rs`, which is this macro's
//! specification-by-example).
//!
//! `#[computation]` turns
//!
//! ```ignore
//! #[computation]
//! async fn sync_file(
//!     ctx: &Ctx,
//!     #[flow] source: &Arc<FsSource>,
//!     #[flow] sink: &Arc<FsSink>,
//!     rel: PathBuf,
//! ) -> Result<(), CompError> {
//!     let bytes = source.read_file(ctx, rel.clone()).await?;
//!     sink.write_file(ctx, rel, bytes).await
//! }
//! ```
//!
//! into four items (see [`expand`] for the exact generated code):
//!
//! 1. A private `impl` function with the user's exact original signature and
//!    body (flows still taken by reference, exactly as written).
//! 2. A public wrapper function, also with the user's exact original
//!    signature, whose body builds the ordered `FlowId` list and the
//!    (possibly tupled) parameter, then calls
//!    `computations::Ctx::eval_flows`.
//! 3. A `FlowThunk` (a plain, capture-free `fn`) that decodes the param
//!    bytes, resolves each `#[flow]` argument from a `FlowResolver` by
//!    position, and calls the `impl` function.
//! 4. An `inventory::submit!` registering `(NAME, thunk)` so
//!    `EngineBuilder::build()` finds it automatically — see
//!    `computations::flow::ComputationEntry`'s docs for the registration
//!    mechanism and its platform caveats.
//!
//! ## Why `#[flow]` must be explicit
//!
//! A proc macro only ever sees tokens, never resolved types: it cannot tell
//! `Arc<FsSource>` (a flow) from `Arc<Vec<u8>>` (an ordinary parameter that
//! happens to be `Arc`-wrapped) without a human-supplied signal. `#[flow]`
//! is that signal. This macro does perform one syntactic check on a
//! `#[flow]`-marked argument (it must be a reference to `Arc<T>`), but it
//! cannot check that `T` actually implements `SourceBase`/`SinkBase` --
//! that surfaces later, as an ordinary Rust trait-bound error, when the
//! generated code tries to call `.instance_id()`-backed helpers on it (see
//! `computations::flow`'s `AsFlowId`/`AsFlowIdSink`/`ResolveFlow`/
//! `ResolveFlowSink` traits).

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    Attribute, FnArg, GenericArgument, Ident, ItemFn, Pat, PathArguments, ReturnType, Type, TypeReference,
    parse_macro_input,
};

#[proc_macro_attribute]
pub fn computation(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[computation] takes no arguments",
        )
        .to_compile_error()
        .into();
    }
    let input = parse_macro_input!(item as ItemFn);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// One `#[flow]`-marked argument, parsed out of the function signature.
struct FlowArg {
    ident: Ident,
    /// The `T` in `&Arc<T>`.
    inner_ty: Type,
}

/// One ordinary (unmarked) parameter argument.
struct ParamArg {
    ident: Ident,
    ty: Type,
}

fn expand(mut input: ItemFn) -> syn::Result<TokenStream2> {
    let sig = &input.sig;

    if sig.asyncness.is_none() {
        return Err(syn::Error::new(sig.fn_token.span(), "#[computation] functions must be `async fn`"));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "#[computation] does not support generic functions",
        ));
    }

    let mut inputs = sig.inputs.iter();

    let ctx_arg = inputs
        .next()
        .ok_or_else(|| syn::Error::new(sig.paren_token.span.join(), "#[computation] functions must take `ctx: &Ctx` as their first argument"))?;
    let ctx_ident = check_ctx_arg(ctx_arg)?;

    let mut flow_args: Vec<FlowArg> = Vec::new();
    let mut param_args: Vec<ParamArg> = Vec::new();

    // Re-walk the full input list (including `ctx`) so we can strip the
    // `#[flow]` helper attribute from the arguments we keep verbatim in the
    // regenerated signatures below -- rustc rejects an unknown attribute
    // left on an argument once this macro's expansion replaces the item.
    let mut new_inputs = syn::punctuated::Punctuated::new();
    for (i, arg) in sig.inputs.iter().enumerate() {
        let FnArg::Typed(pat_type) = arg else {
            return Err(syn::Error::new(arg.span(), "#[computation] functions cannot take `self`"));
        };
        if i == 0 {
            // Already validated above; keep as-is.
            new_inputs.push(arg.clone());
            continue;
        }
        let (flow_attrs, keep_attrs): (Vec<Attribute>, Vec<Attribute>) =
            pat_type.attrs.iter().cloned().partition(|a| a.path().is_ident("flow"));

        let ident = match pat_type.pat.as_ref() {
            Pat::Ident(pat_ident) => pat_ident.ident.clone(),
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    "#[computation] argument patterns must be simple identifiers",
                ));
            }
        };

        let mut cleaned = pat_type.clone();
        cleaned.attrs = keep_attrs;
        new_inputs.push(FnArg::Typed(cleaned.clone()));

        if flow_attrs.is_empty() {
            param_args.push(ParamArg {
                ident,
                ty: (*pat_type.ty).clone(),
            });
        } else {
            let inner_ty = extract_flow_inner_ty(&pat_type.ty)?;
            flow_args.push(FlowArg { ident, inner_ty });
        }
    }

    let ret_ty = extract_result_ok_ty(&sig.output)?;

    let fn_name = sig.ident.clone();
    let vis = input.vis.clone();
    let attrs: Vec<Attribute> = input.attrs.clone();

    // Public (matching the annotated function's own visibility), and named
    // predictably (`<FN_NAME_UPPER>_NAME`) rather than doc-hidden: this is
    // deliberately part of a `#[computation]` function's public surface, not
    // an internal implementation detail -- it's what lets a caller drive the
    // computation as a genuine root via `Engine::run_flows`/`eval_root_flows`
    // (needing a name plus a `FlowId` list, built from the public
    // `computations::flow::AsFlowId`/`AsFlowIdSink` traits) with no
    // `Comp<P, R>` handle and no `EngineBuilder::define*` call at all — see
    // `computations-fs/examples/dirsync.rs` for exactly that pattern.
    let name_const = format_ident!("{}_NAME", fn_name.to_string().to_uppercase());
    let impl_fn = format_ident!("__computation_impl_{}", fn_name);
    let thunk_fn = format_ident!("__computation_thunk_{}", fn_name);
    let register_fn = format_ident!("__computation_register_{}", fn_name);

    let fn_name_str = fn_name.to_string();

    // The impl fn: the user's original body, verbatim, under the original
    // (attribute-stripped) signature.
    input.sig.ident = impl_fn.clone();
    input.sig.inputs = new_inputs.clone();
    input.vis = syn::Visibility::Inherited;
    input.attrs.clear();
    let impl_item = input;

    // -- The wrapper: same signature as the user wrote, calling `eval_flows`. --
    let flow_idents: Vec<&Ident> = flow_args.iter().map(|f| &f.ident).collect();
    let param_idents: Vec<&Ident> = param_args.iter().map(|p| &p.ident).collect();
    let param_tys: Vec<&Type> = param_args.iter().map(|p| &p.ty).collect();
    let n_flows = flow_args.len();

    let param_expr = match param_idents.len() {
        0 => quote! { () },
        1 => {
            let p = param_idents[0];
            quote! { #p }
        }
        _ => quote! { ( #(#param_idents),* ) },
    };
    let param_ty = match param_tys.len() {
        0 => quote! { () },
        1 => {
            let t = param_tys[0];
            quote! { #t }
        }
        _ => quote! { ( #(#param_tys),* ) },
    };

    let wrapper = quote! {
        #(#attrs)*
        #vis async fn #fn_name(#new_inputs) -> Result<#ret_ty, ::computations::error::CompError> {
            #[allow(unused_imports)]
            use ::computations::flow::{AsFlowId as _, AsFlowIdSink as _};
            let __flows: [::computations::FlowId; #n_flows] = [ #( #flow_idents.as_flow_id() ),* ];
            let __param: #param_ty = #param_expr;
            #ctx_ident.eval_flows(#name_const, &__flows, __param).await
        }
    };

    // -- The thunk: decodes param bytes, resolves flows, calls the impl fn. --
    let param_destructure = match param_idents.len() {
        0 => quote! {},
        1 => {
            let p = param_idents[0];
            quote! { let #p: #param_ty = __param; }
        }
        _ => quote! { let ( #(#param_idents),* ): #param_ty = __param; },
    };

    let flow_resolutions: Vec<TokenStream2> = flow_args
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let ident = &f.ident;
            let inner_ty = &f.inner_ty;
            quote! {
                let #ident: ::std::sync::Arc<#inner_ty> =
                    match ::std::sync::Arc::<#inner_ty>::resolve_flow(&__resolver, #idx) {
                        Ok(v) => v,
                        Err(e) => return ::std::boxed::Box::pin(async move { Err(e) }),
                    };
            }
        })
        .collect();

    let call_args: Vec<TokenStream2> = {
        let mut v = Vec::new();
        v.push(quote! { &__ctx });
        for f in &flow_args {
            let ident = &f.ident;
            v.push(quote! { &#ident });
        }
        for p in &param_args {
            let ident = &p.ident;
            v.push(quote! { #ident });
        }
        v
    };

    let thunk = quote! {
        #[doc(hidden)]
        fn #thunk_fn(
            __ctx: ::computations::Ctx,
            __resolver: ::computations::FlowResolver<'_>,
            __param_bytes: &[u8],
        ) -> ::computations::FlowThunkFut {
            #[allow(unused_imports)]
            use ::computations::flow::{ResolveFlow as _, ResolveFlowSink as _};
            let __param: #param_ty = match ::computations::postcard::from_bytes(__param_bytes) {
                Ok(p) => p,
                Err(e) => {
                    return ::std::boxed::Box::pin(async move {
                        Err(::computations::error::CompError::Failed(format!(
                            "{}: param decode failed: {}",
                            #name_const, e
                        )))
                    });
                }
            };
            #param_destructure
            #(#flow_resolutions)*
            ::std::boxed::Box::pin(async move {
                let __result = #impl_fn(#(#call_args),*).await?;
                Ok(::std::sync::Arc::new(__result) as ::std::sync::Arc<dyn ::std::any::Any + Send + Sync>)
            })
        }
    };

    let registration = quote! {
        #[doc(hidden)]
        fn #register_fn(builder: &mut ::computations::EngineBuilder) {
            builder.define_flows::<#ret_ty>(#name_const, #thunk_fn);
        }

        ::computations::inventory::submit! {
            ::computations::flow::ComputationEntry {
                name: #name_const,
                register: #register_fn,
            }
        }
    };

    let name_const_item = quote! {
        /// This computation's globally-unique registered name (see
        /// `computations::flow`'s module docs): `concat!(module_path!(), "::", "..")`,
        /// evaluated here so it reflects wherever this function is actually
        /// defined. Useful for driving this computation as a root via
        /// `computations::Engine::run_flows`/`eval_root_flows` directly.
        #vis const #name_const: &str = concat!(module_path!(), "::", #fn_name_str);
    };

    Ok(quote! {
        #name_const_item
        #impl_item
        #wrapper
        #thunk
        #registration
    })
}

/// Checks that `arg` (the first argument) is `ctx: &Ctx` or `ctx: Ctx`,
/// returning its binding identifier.
fn check_ctx_arg(arg: &FnArg) -> syn::Result<Ident> {
    let FnArg::Typed(pat_type) = arg else {
        return Err(syn::Error::new(arg.span(), "#[computation] functions cannot take `self`"));
    };
    let ident = match pat_type.pat.as_ref() {
        Pat::Ident(pat_ident) => pat_ident.ident.clone(),
        other => {
            return Err(syn::Error::new(other.span(), "#[computation]'s first argument must be a simple identifier"));
        }
    };
    let ty: &Type = &pat_type.ty;
    let is_ctx_type = |t: &Type| -> bool {
        matches!(t, Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Ctx"))
    };
    let ok = match ty {
        Type::Reference(TypeReference { elem, .. }) => is_ctx_type(elem),
        other => is_ctx_type(other),
    };
    if !ok {
        return Err(syn::Error::new(
            ty.span(),
            "#[computation]'s first argument must be `&Ctx` (or `Ctx`)",
        ));
    }
    Ok(ident)
}

/// Extracts `T` from a `#[flow]` argument's type, which must be `&Arc<T>`
/// (a reference to `Arc<T>`).
fn extract_flow_inner_ty(ty: &Type) -> syn::Result<Type> {
    let err = || {
        syn::Error::new(
            ty.span(),
            "#[flow] arguments must be a reference to `Arc<T>` where `T` implements `SourceBase` or `SinkBase` \
             (e.g. `#[flow] source: &Arc<FsSource>`)",
        )
    };
    let Type::Reference(TypeReference { elem, .. }) = ty else {
        return Err(err());
    };
    let Type::Path(type_path) = elem.as_ref() else {
        return Err(err());
    };
    let Some(last) = type_path.path.segments.last() else {
        return Err(err());
    };
    if last.ident != "Arc" {
        return Err(err());
    }
    let PathArguments::AngleBracketed(generics) = &last.arguments else {
        return Err(err());
    };
    let mut type_args = generics.args.iter().filter_map(|a| match a {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    });
    let Some(inner) = type_args.next() else {
        return Err(err());
    };
    if type_args.next().is_some() {
        return Err(err());
    }
    Ok(inner)
}

/// Extracts `R` from a computation's declared return type, which must be
/// exactly `Result<R, CompError>` (allowing `CompError` to be spelled with
/// any path prefix, e.g. `computations::error::CompError`).
fn extract_result_ok_ty(output: &ReturnType) -> syn::Result<Type> {
    let err_span = |s: proc_macro2::Span| {
        syn::Error::new(s, "#[computation] functions must return `Result<R, CompError>`")
    };
    let ReturnType::Type(_, ty) = output else {
        return Err(err_span(proc_macro2::Span::call_site()));
    };
    let Type::Path(type_path) = ty.as_ref() else {
        return Err(err_span(ty.span()));
    };
    let Some(last) = type_path.path.segments.last() else {
        return Err(err_span(ty.span()));
    };
    if last.ident != "Result" {
        return Err(err_span(ty.span()));
    }
    let PathArguments::AngleBracketed(generics) = &last.arguments else {
        return Err(err_span(ty.span()));
    };
    let type_args: Vec<Type> = generics
        .args
        .iter()
        .filter_map(|a| match a {
            GenericArgument::Type(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    if type_args.len() != 2 {
        return Err(err_span(ty.span()));
    }
    let ok_ty = type_args[0].clone();
    let err_ty = &type_args[1];
    let Type::Path(err_path) = err_ty else {
        return Err(err_span(err_ty.span()));
    };
    let is_comp_error = err_path.path.segments.last().is_some_and(|s| s.ident == "CompError");
    if !is_comp_error {
        return Err(err_span(err_ty.span()));
    }
    Ok(ok_ty)
}
