// cxx bridge for ATree<u64>
#[cxx::bridge]
mod ffi {
    // Typed enums and structs exposed to C++.
    #[namespace = "a_tree_ffi"]
    enum AttributeKind {
        Boolean = 0,
        Integer = 1,
        Float = 2,
        String = 3,
        StringList = 4,
        IntegerList = 5,
    }

    #[namespace = "a_tree_ffi"]
    struct AttributeDefinition {
        name: String,
        kind: AttributeKind,
    }

    #[namespace = "a_tree_ffi"]
    enum EventValueTag {
        Undefined = 0,
        Boolean = 1,
        Integer = 2,
        Float = 3,
        String = 4,
        StringList = 5,
        IntegerList = 6,
    }

    #[namespace = "a_tree_ffi"]
    struct EventValue {
        tag: EventValueTag,
        boolean: bool,
        integer: i64,
        float_value: f64,
        string_value: String,
        string_list: Vec<String>,
        integer_list: Vec<i64>,
    }

    #[namespace = "a_tree_ffi"]
    struct EventAttribute {
        name: String,
        value: EventValue,
    }

    #[namespace = "a_tree_ffi"]
    struct Event {
        attributes: Vec<EventAttribute>,
    }

    // Opaque tree type (Rust owned). C++ will hold a UniquePtr/Reference.
    extern "Rust" {
        type ATreeU64;

        // Create a new tree from a vector of AttributeDefinition.
        // Returns Box<ATreeU64> on success (mapped to UniquePtr on C++ side).
        fn atree_new(defs: Vec<AttributeDefinition>) -> Result<Box<ATreeU64>>;

        // Insert expression associated with subscription id.
        fn atree_insert(atree: &mut ATreeU64, subscription_id: u64, expression: &str) -> Result<()>;

        // Search using a typed Event; returns a Vec<u64> of matching subscription IDs.
        fn atree_search(atree: &ATreeU64, event: Event) -> Result<Vec<u64>>;
    }
}

// Rust side wrapper type
pub struct ATreeU64(pub a_tree::ATree<u64>);

// Helper to convert ffi::AttributeDefinition into crate AttributeDefinition
fn convert_attribute_definition(def: &ffi::AttributeDefinition) -> Result<a_tree::events::AttributeDefinition, String> {
    use ffi::AttributeKind::*;
    use a_tree::events::AttributeDefinition as AD;

    match def.kind {
        AttributeKind::Boolean => Ok(AD::boolean(&def.name)),
        AttributeKind::Integer => Ok(AD::integer(&def.name)),
        AttributeKind::Float => Ok(AD::float(&def.name)),
        AttributeKind::String => Ok(AD::string(&def.name)),
        AttributeKind::StringList => Ok(AD::string_list(&def.name)),
        AttributeKind::IntegerList => Ok(AD::integer_list(&def.name)),
    }
}

fn apply_event_to_builder(builder: &mut a_tree::events::EventBuilder, attr: &ffi::EventAttribute) -> Result<(), String> {
    use ffi::EventValueTag::*;
    let name = &attr.name;
    match attr.value.tag {
        EventValueTag::Undefined => Ok(()),
        EventValueTag::Boolean => builder.with_boolean(name, attr.value.boolean).map_err(|e| format!("{:?}", e)),
        EventValueTag::Integer => builder.with_integer(name, attr.value.integer).map_err(|e| format!("{:?}", e)),
        EventValueTag::Float => builder.with_float(name, attr.value.float_value).map_err(|e| format!("{:?}", e)),
        EventValueTag::String => builder.with_string(name, &attr.value.string_value).map_err(|e| format!("{:?}", e)),
        EventValueTag::StringList => builder.with_string_list(name, &attr.value.string_list.iter().map(|s| s.as_str()).collect::<Vec<_>>()).map_err(|e| format!("{:?}", e)),
        EventValueTag::IntegerList => builder.with_integer_list(name, &attr.value.integer_list).map_err(|e| format!("{:?}", e)),
    }
}

impl ATreeU64 {
    // Create from cxx AttributeDefinition vector
    pub fn atree_new(defs: Vec<ffi::AttributeDefinition>) -> Result<Box<ATreeU64>, String> {
        // Convert to crate attribute defs
        let mut rust_defs: Vec<a_tree::events::AttributeDefinition> = Vec::with_capacity(defs.len());
        for d in &defs {
            rust_defs.push(convert_attribute_definition(d)?);
        }
        let tree = a_tree::ATree::<u64>::new(&rust_defs).map_err(|e| format!("{:?}", e))?;
        Ok(Box::new(ATreeU64(tree)))
    }

    // Insert expression
    pub fn atree_insert(atree: &mut ATreeU64, subscription_id: u64, expression: &str) -> Result<(), String> {
        atree.0.insert(&subscription_id, expression).map_err(|e| format!("{:?}", e))
    }

    // Search with Event -> returns Vec<u64> of matching subscription IDs
    pub fn atree_search(atree: &ATreeU64, event: ffi::Event) -> Result<Vec<u64>, String> {
        // Build the Event using the tree's attribute table / string table
        let mut builder = atree.0.make_event();
        for attr in &event.attributes {
            apply_event_to_builder(&mut builder, attr)?;
        }
        let event = builder.build().map_err(|e| format!("{:?}", e))?;
        let report = atree.0.search(&event).map_err(|e| format!("{:?}", e))?;
        // report.matches() yields slice of &u64 or similar; collect by cloning
        let mut result: Vec<u64> = Vec::new();
        for id in report.matches() {
            result.push(*id);
        }
        Ok(result)
    }
}

pub fn atree_free(_: Box<ATreeU64>) {
    // Box is dropped here when this function returns, freeing the Rust-side object.
}
