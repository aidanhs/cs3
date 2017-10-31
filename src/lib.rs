#![feature(process_abort)]

extern crate rusoto_core;
extern crate rusoto_s3;

use std::error::Error;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;
use std::time::Instant;

use rusoto_s3::{S3, S3Client, PutObjectRequest};

use rusoto_core::EnvironmentProvider;
use rusoto_core::default_tls_client;
use rusoto_core::region::Region;

#[no_mangle]
pub extern "C" fn s3_put(bucket: *const c_char, key: *const c_char, body: *const u8, body_len: u64) -> bool {
	let begin = Instant::now();
	match s3_put_inner(bucket, key, body, body_len) {
		Ok(output) => {
			let time_taken = begin.elapsed();
			println!("Successful s3 upload took {:?}: {:?}", time_taken, output);
			true
		},
		Err(error) => {
			println!("Failed s3 upload of {:?}", error);
			false
		},
	}
}

fn s3_put_inner(bucket: *const c_char, key: *const c_char, body: *const u8, body_len: u64) -> Result<String, Box<Error>> {
	let bucket = unsafe { CStr::from_ptr(bucket).to_str()? };
	let key = unsafe { CStr::from_ptr(key).to_str()? };
	let body = unsafe { slice::from_raw_parts(body, body_len as usize) };

	// Set vars here
	let provider = EnvironmentProvider;
	let client = S3Client::new(default_tls_client().unwrap(), provider, Region::UsEast1);
	let mut put_object_request: PutObjectRequest = Default::default();
	put_object_request.bucket = bucket.to_owned();
	put_object_request.key = key.to_owned();
	put_object_request.body = Some(body.to_owned());
	let res = client.put_object(&put_object_request)?;
	Ok(format!("{:?}", res))
}
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
