//! Logging utilities

use std::str;

static mut DEBUG_HOOK: Option<&dyn DebugSink> = None;
static mut LOG_HOOK: Option<&dyn LoggingSink> = None;

pub trait DebugSink {
    fn debug(&self, topic: &str, comment: &str);
}

pub trait LoggingSink {
    fn debug(&self, topic: &str, comment: &str);
}

pub fn aggravation(first: i32, second: i32) -> i32
{
    println!("WHAT AGGRAVATION");

    first + second
}

pub fn add_debug_hook(hook: &'static dyn DebugSink) {
    unsafe {
        DEBUG_HOOK = Some(hook);
    }
    ()
}

pub fn add_logging_hook(hook: &'static dyn LoggingSink) {
    unsafe {
        LOG_HOOK = Some(hook);
    }
    ()
}