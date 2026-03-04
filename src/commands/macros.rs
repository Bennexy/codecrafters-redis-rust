#[macro_export]
macro_rules! redis_commands {
    ($($name:ident => $cmd:ty),+ $(,)?) => {
        pub enum UnparsedCommandType {
            $(
                $name(Command<Unparsed, $cmd>),
            )+
        }

        pub enum ParsedCommandType {
            $(
                $name(Command<Parsed, $cmd>),
            )+
        }

        impl UnparsedCommandType {
            pub fn name(&self) -> String {
                match self {
                    $(
                        UnparsedCommandType::$name(_) => stringify!($name).to_lowercase(),
                    )+
                }
            }

            pub fn parse(self) -> Result<ParsedCommandType, RedisMessageType> {
                match self {
                    $(
                        UnparsedCommandType::$name(cmd) =>
                            Ok(ParsedCommandType::$name(cmd.parse()?)),
                    )+
                }
            }

            #[inline(always)]
            pub fn from_bytes(cmd_bytes: &[u8], args: VecDeque<RedisMessageType>) -> Result<Self, RedisMessageType> {
                match cmd_bytes.len() {
                    $(
                        l if l == stringify!($name).len() && cmd_bytes.eq_ignore_ascii_case(stringify!($name).as_bytes()) =>
                            Ok(UnparsedCommandType::$name(Command::<Unparsed, $cmd>::new(args))),
                    )+
                    _ => Err(RedisMessageType::error(format!(
                        "Unknown command: '{}'",
                        String::from_utf8_lossy(cmd_bytes)
                    ))),
                }
            }
        }

        impl ParsedCommandType {
            pub fn execute(self) -> Result<RedisMessageType, RedisMessageType> {
                match self {
                    $(
                        ParsedCommandType::$name(cmd) => cmd.execute(),
                    )+
                }
            }
        }
    };
}
