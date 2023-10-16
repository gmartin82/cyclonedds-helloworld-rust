use cyclors::*;
use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::{thread, time};

const DDS_DOMAIN: u32 = 0;
const DDS_NEVER: i64 = i64::MAX;
const TOPIC_NAME: &str = "HelloWorldData_Msg";

fn main() {
    unsafe {
        let participant = dds_create_participant(DDS_DOMAIN, std::ptr::null(), std::ptr::null());
        if participant < 0 {
            println!(
                "Failed to create domain participant: {}",
                CStr::from_ptr(dds_strretcode(participant))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            );
            std::process::exit(1);
        }

        let topic = find_topic(participant, TOPIC_NAME);
        let reader = dds_create_reader(participant, topic, std::ptr::null(), std::ptr::null());

        let mut zp: *mut ddsi_serdata = std::ptr::null_mut();
        #[allow(clippy::uninit_assumed_init)]
        let mut si = MaybeUninit::<dds_sample_info_t>::uninit();

        loop {
            let one_secs = time::Duration::from_secs(1);
            thread::sleep(one_secs);

            /* When run with IOX PSMX plugin configured received samples fail to be added to the
             * reader history cache and so no samples will be received. The issue seems to be
             * caused by the Cyclone DDS handle server not being initialized when the C++ IOX
             * plugin calls back to the core Cyclone DDS C code.
             * 
             * This is not an issue for the Cyclone DDS examples and so may be due to the
             * involvement of RUST.
             * 
             * Where the IOX PSMX plugin is not enabled this example receives data as expected.
             */
            if dds_takecdr(reader, &mut zp, 1, si.as_mut_ptr(), DDS_ANY_STATE) > 0 {
                let si = si.assume_init();
                if si.valid_data {
                    println!("dds_takecdr() - received valid data");
                }
                ddsi_serdata_unref(zp);
            }
        }
    }
}

unsafe fn find_topic(participant: dds_entity_t, topic_name: &str) -> dds_entity_t {
    let waitset = dds_create_waitset(participant);
    let dcpspublication_reader = dds_create_reader(
        participant,
        DDS_BUILTIN_TOPIC_DCPSPUBLICATION,
        std::ptr::null(),
        std::ptr::null(),
    );
    let dcpspublication_readcond = dds_create_readcondition(dcpspublication_reader, DDS_ANY_STATE);
    let _ = dds_waitset_attach(waitset, dcpspublication_readcond, 0);

    let mut topic: dds_entity_t = -1;

    println!("Waiting for topic with name {}", topic_name);
    while topic < 0 && dds_waitset_wait_until(waitset, std::ptr::null_mut(), 0, DDS_NEVER) > 0 {
        #[allow(clippy::uninit_assumed_init)]
        let mut si = MaybeUninit::<dds_sample_info_t>::uninit();
        let mut sample: *mut ::std::os::raw::c_void = std::ptr::null_mut();

        if dds_take(dcpspublication_reader, &mut sample, si.as_mut_ptr(), 1, 1) <= 0 {
            continue;
        }
        let si = si.assume_init();

        if !si.valid_data {
            dds_return_loan(dcpspublication_reader, &mut sample, 1);
            continue;
        }

        let endpont_sample = sample as *mut dds_builtintopic_endpoint_t;

        let current_topic_name = match CStr::from_ptr((*endpont_sample).topic_name).to_str() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Discovery of an invalid topic name: {}", e);
                dds_return_loan(dcpspublication_reader, &mut sample, 1);
                continue;
            }
        };

        println!("Discovered DDS publication on topic {}", current_topic_name);

        if current_topic_name != topic_name {
            dds_return_loan(dcpspublication_reader, &mut sample, 1);
            continue;
        }

        let mut type_info: *const dds_typeinfo_t = std::ptr::null();
        let ret = dds_builtintopic_get_endpoint_type_info(endpont_sample, &mut type_info);

        if ret != 0 || type_info.is_null() {
            eprintln!(
                "Type information not available for topic {}",
                current_topic_name
            );
            dds_return_loan(dcpspublication_reader, &mut sample, 1);
            continue;
        }

        let mut descriptor: *mut dds_topic_descriptor_t = std::ptr::null_mut();
        let ret = dds_create_topic_descriptor(
            dds_find_scope_DDS_FIND_SCOPE_GLOBAL,
            participant,
            type_info,
            200000000,
            &mut descriptor,
        );

        if ret != 0 {
            eprintln!(
                "Failed to create topic descriptor for topic {}",
                current_topic_name
            );
            dds_return_loan(dcpspublication_reader, &mut sample, 1);
            continue;
        }
        topic = dds_create_topic(
            participant,
            descriptor,
            (*endpont_sample).topic_name,
            std::ptr::null(),
            std::ptr::null(),
        );
        dds_delete_topic_descriptor(descriptor);

        if topic < 0 {
            eprintln!("Failed to find topic {}", topic_name);
            dds_return_loan(dcpspublication_reader, &mut sample, 1);
            continue;
        }
        dds_return_loan(dcpspublication_reader, &mut sample, 1);
    }
    dds_delete(dcpspublication_reader);
    dds_delete(waitset);
    println!("Found topic with name {}", topic_name);
    topic
}
