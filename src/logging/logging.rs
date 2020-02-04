//! Logging utilities

use std::str;

static mut DEBUG_HOOK: Option<&dyn DebugSink> = None;
static mut LOG_HOOK: Option<&dyn LoggingSink> = None;

pub trait DebugSink {
    fn debug(&self, topic: &str, comment: &str);
}

pub trait LoggingSink {
    fn log(&self, comment: &str);
}

pub fn add_debug_hook(hook: &'static dyn DebugSink) {
    unsafe {
        match DEBUG_HOOK {
        None => DEBUG_HOOK = Some(hook),
        Some(_) => ()
        }
    };
    ()
}

pub fn debug(topic: &str, content: &str) {
    unsafe {
        match DEBUG_HOOK {
            None => println!("{} {}", topic, content),
            Some(dh) => dh.debug(topic, content)
        }
    };
    ()
}


pub fn add_logging_hook(hook: &'static dyn LoggingSink) {
    unsafe {
        match LOG_HOOK {
        None => LOG_HOOK = Some(hook),
        Some(_) => ()
        }
    };
    ()
}

pub fn log(content: &str) {
    unsafe {
        match LOG_HOOK {
            None => println!("{}", content),
            Some(lh) => lh.log(content)
        }
    };
    ()
}
