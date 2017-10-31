#![feature(process_abort)]

extern crate libc;
extern crate nix;
extern crate s3;

use std::env;
use std::error::Error;
use std::ffi::CStr;
use std::mem;
use std::os::raw::c_char;
use std::process;
use std::slice;
use std::time::{Duration, Instant};

use libc::pid_t;

use nix::sys::wait::WaitStatus;
use nix::unistd::ForkResult;

use s3::bucket::Bucket;
use s3::credentials::Credentials;
use s3::region::Region;

fn load_credentials() -> Credentials {
	let aws_access = env::var("AWS_ACCESS_KEY_ID").expect("Must specify AWS_ACCESS_KEY_ID");
	let aws_secret = env::var("AWS_SECRET_ACCESS_KEY").expect("Must specify AWS_SECRET_ACCESS_KEY");
	Credentials::new(&aws_access, &aws_secret, None)
}

#[no_mangle]
pub extern "C" fn s3_put(bucket: *const c_char, key: *const c_char, body: *const u8, body_len: u64) -> pid_t {
	match nix::unistd::fork() {
		Ok(ForkResult::Parent { child }) => {
			unsafe { mem::transmute(child) }
		}
		Ok(ForkResult::Child) => {
			let begin = Instant::now();
			let ret = s3_put_inner(bucket, key, body, body_len).map(|v| v.to_string()).map_err(|v| v.to_string());
			let time_taken = begin.elapsed();
			process::exit(match ret {
				Ok(s) => { println!("Successful s3 upload took {:?}: {}", time_taken, s); 60 },
				Err(s) => { println!("Failed s3 upload took {:?}: {}", time_taken, s); 61 },
			})
		},
		Err(_) => { println!("Fork failed"); process::abort() },
	}
}

#[no_mangle]
pub extern "C" fn s3_put_poll(pid: pid_t) -> u64 {
	let pid = nix::unistd::Pid::from_raw(pid);
	let res = match nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WNOHANG)) {
		Ok(r) => r,
		Err(e) => { println!("waitpid failed: {}", e); process::abort() },
	};
	match res {
		WaitStatus::Exited(expid, code) => {
			assert_eq!(pid, expid);
			match code {
				60 => 0, // success
				61 => 1, // fail
				_ => { println!("Unknown termination of upload process"); process::abort() },
			}
		},
		WaitStatus::StillAlive => 2, // try again
		WaitStatus::Signaled(_, _, _) |
		WaitStatus::Stopped(_, _) |
		WaitStatus::PtraceEvent(_, _, _) |
		WaitStatus::PtraceSyscall(_) |
		WaitStatus::Continued(_) => { println!("Bad status of upload process: {:?}", res); process::abort() },
	}
}

fn s3_put_inner(bucket: *const c_char, key: *const c_char, body: *const u8, body_len: u64) -> Result<String, Box<Error>> {
	let bucket = unsafe { CStr::from_ptr(bucket).to_str()? };
	let key = unsafe { CStr::from_ptr(key).to_str()? };
	let body = unsafe { slice::from_raw_parts(body, body_len as usize) };
	let credentials = load_credentials();
	let bucket = Bucket::new(bucket, Region::UsEast1, credentials);
	let (_, code) = bucket.put(key, body, "text/plain")?;
	Ok(format!("{:?}", code))
}

// RUSOTO DOES A LOT OF ALLOCATION, PARTICULARLY THE BODY
//use rusoto_s3::{S3, S3Client, PutObjectRequest};
//
//use rusoto_core::EnvironmentProvider;
//use rusoto_core::default_tls_client;
//use rusoto_core::region::Region;
//#[no_mangle]
//pub extern "C" fn s3_put(bucket: *const c_char, key: *const c_char, body: *const u8, body_len: u64) -> *const mpsc::Receiver<(Duration, Result<String, String>)> {
//	let (tx, rx) = mpsc::sync_channel(0);
//	let child = thread::spawn(move || {
//		let begin = Instant::now();
//		let ret = s3_put_inner(bucket, key, body, body_len).map(|v| v.to_string()).map_err(|v| v.to_string());
//		let time_taken = begin.elapsed();
//		tx.send((time_taken, ret))
//	});
//	Box::new(rx).into_raw()
//}
//
//#[no_mangle]
//pub extern "C" fn s3_put_poll(rx: *mut mpsc::Receiver<(Duration, Result<String, String>)>) -> u64 {
//	match unsafe { &*rx }.try_recv() {
//		Ok((time_taken, ret)) => {
//			match ret {
//				Ok(s) => { println!("Successful s3 upload took {:?}: {}", time_taken, s); 0 },
//				Err(s) => { println!("Failed s3 upload took {:?}: {}", time_taken, s); 1 },
//			}
//			Box::from_raw(rx); // drop
//		},
//		Err(mpsc::TryRecvError::Empty) => 2,
//		Err(mpsc::TryRecvError::Disconnected) => {
//			println!("S3 upload thread crashed?");
//			abort!()
//		},
//	}
//}
//
//fn s3_put_inner(bucket: *const c_char, key: *const c_char, body: *const u8, body_len: u64) -> Result<String, Box<Error>> {
//	let bucket = unsafe { CStr::from_ptr(bucket).to_str()? };
//	let key = unsafe { CStr::from_ptr(key).to_str()? };
//	let body = unsafe { slice::from_raw_parts(body, body_len as usize) };
//
//	let provider = EnvironmentProvider;
//	let client = S3Client::new(default_tls_client().unwrap(), provider, Region::UsEast1);
//	let mut put_object_request: PutObjectRequest = Default::default();
//	put_object_request.bucket = bucket.to_owned();
//	put_object_request.key = key.to_owned();
//	put_object_request.body = Some(body.to_owned());
//	let res = client.put_object(&put_object_request)?;
//	Ok(format!("{:?}", res))
//}




//fn s3_multipart_create(bucket: &str, key: &str) -> *mut String {
//	//let bucket = "hadean".to_string();
//	//let key = "multipart_test".to_string();
//	let mut create_multipart_upload_request: CreateMultipartUploadRequest = Default::default();
//	create_multipart_upload_request.bucket = bucket.clone();
//	create_multipart_upload_request.key = key.clone();
//
//	match client.create_multipart_upload(&create_multipart_upload_request) {
//		Ok(output) => {
//			match output.upload_id {
//				Some(id) => Box::new(upload_id).into_raw(),
//				None => { println!("no id when creating multipart"); ptr::null_mut() },
//			}
//		},
//		Err(error) => { println!("s3 error: {}", error); ptr::null_mut(),
//	}
//}
//fn s3_upload_part(bucket: &str, key: &str, upload_id: *mut String, part_number: i64, len: i64) {
//	let mut upload_part_request: UploadPartRequest = Default::default();
//	upload_part_request.bucket = bucket.clone();
//	upload_part_request.upload_id = upload_id.clone();
//	upload_part_request.part_number = part_number;
//	upload_part_request.content_length = Some(len);
//
//	upload_part_request.key = key.clone();
//
//	let my_content: Vec<u8> = vec![];
//	//"abcdefghijklmnopqrstuvwxyz".to_string().into_bytes();
//
//	upload_part_request.body = Some(my_content.clone());
//
//	let mut etag = "".to_string();
//
//	let begin = Instant::now();
//
//	match client.upload_part(&upload_part_request) {
//		Ok(output) => {
//			match output.e_tag {
//				Some(e_tag) => etag = e_tag,
//				None => println!("no e_tag"),
//			}
//		}
//		Err(error) => println!("{}", error),
//	}
//
//	let mut multipart_upload: Vec<CompletedPart> = Vec::new();
//	multipart_upload.push(
//		CompletedPart { e_tag: Some(etag), part_number: Some(1i64), }
//		);
//
//	let mut completed_multipart_upload: CompletedMultipartUpload = Default::default();
//	completed_multipart_upload.parts = Some(multipart_upload);
//
//	println!("d");
//	let mut complete_multipart_upload_request: CompleteMultipartUploadRequest = Default::default();
//	complete_multipart_upload_request.bucket = bucket;
//	complete_multipart_upload_request.key = key.clone();
//	complete_multipart_upload_request.upload_id = upload_id.clone();
//	complete_multipart_upload_request.multipart_upload = Some(completed_multipart_upload);
//	println!("e");
//	match client.complete_multipart_upload(&complete_multipart_upload_request) {
//		Ok(output) => {
//			match output.e_tag {
//				Some(e_tag) => println!("{}", e_tag),
//				None => println!("no e_tag"),
//			}
//		}
//		Err(error) => println!("{}", error),
//
//	}
//
//	let end = begin.elapsed();
//
//	println!("Put took {:?}. Multipart took {:?}", ende, end);
//
//	println!("Upload took {:?}.", ende);
//
//}
