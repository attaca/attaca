use attaca::Open;
use url::Url;

use super::*;
use config::StoreConfig;

macro_rules! backend_remote_add {
    (@inner $url:expr, $($lcname:ident, $ccname:ident : $type:ty),*) => {
        {
            match $url.scheme() {
                $(scheme if <$type>::SCHEMES.contains(&scheme) => StoreKind::$ccname,)*
                _ => bail!("no matching store for URL scheme"),
            }
        }
    };
    ($url:expr) => { all_backends!(backend_remote_add!(@inner $url)) };
}

pub fn add<B: Backend>(this: &mut Repository<B>, name: Name, url: Url) -> FutureUnit {
    let blocking = async_block! {
        let mut config = this.get_config()?;
        ensure!(!config.remotes.contains_key(name.as_str()), "remote already exists");
        let kind = backend_remote_add!(url);
        config.remotes.insert(name.into_string(), StoreConfig { url, kind });
        this.set_config(&config)?;
        Ok(())
    };

    Box::new(blocking)
}
