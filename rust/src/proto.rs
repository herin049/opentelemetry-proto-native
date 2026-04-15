pub(crate) mod common {
    pub(crate) mod v1 {
        include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.common.v1.rs"));
    }
}

pub(crate) mod resource {
    pub(crate) mod v1 {
        include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.resource.v1.rs"));
    }
}

pub(crate) mod trace {
    pub(crate) mod v1 {
        include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
    }
}

pub(crate) mod metrics {
    pub(crate) mod v1 {
        include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.metrics.v1.rs"));
    }
}

pub(crate) mod logs {
    pub(crate) mod v1 {
        include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
    }
}

pub(crate) mod profiles {
    pub(crate) mod v1development {
        include!(concat!(
            env!("OUT_DIR"),
            "/opentelemetry.proto.profiles.v1development.rs"
        ));
    }
}

pub(crate) mod collector {
    pub(crate) mod trace {
        pub(crate) mod v1 {
            include!(concat!(
                env!("OUT_DIR"),
                "/opentelemetry.proto.collector.trace.v1.rs"
            ));
        }
    }

    pub(crate) mod metrics {
        pub(crate) mod v1 {
            include!(concat!(
                env!("OUT_DIR"),
                "/opentelemetry.proto.collector.metrics.v1.rs"
            ));
        }
    }

    pub(crate) mod logs {
        pub(crate) mod v1 {
            include!(concat!(
                env!("OUT_DIR"),
                "/opentelemetry.proto.collector.logs.v1.rs"
            ));
        }
    }

    pub(crate) mod profiles {
        pub(crate) mod v1development {
            include!(concat!(
                env!("OUT_DIR"),
                "/opentelemetry.proto.collector.profiles.v1development.rs"
            ));
        }
    }
}
