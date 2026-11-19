/// Generated client implementations.
pub mod kv_event_service_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value
    )]
    use tonic::codegen::http::Uri;
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct KvEventServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl KvEventServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> KvEventServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> KvEventServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                    http::Request<tonic::body::Body>,
                    Response = http::Response<
                        <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                    >,
                >,
            <T as tonic::codegen::Service<http::Request<tonic::body::Body>>>::Error:
                Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            KvEventServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// The stream's identity, its history window and the tree size.
        pub async fn get_kv_stream_info(
            &mut self,
            request: impl tonic::IntoRequest<super::GetKvStreamInfoRequest>,
        ) -> std::result::Result<tonic::Response<super::KvStreamInfo>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/sglang.kv.v1.KvEventService/GetKvStreamInfo",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "sglang.kv.v1.KvEventService",
                "GetKvStreamInfo",
            ));
            self.inner.unary(req, path, codec).await
        }
        /// Follow the stream. The first item is `start`. With a cursor inside the
        /// history window the missed batches follow, then live batches. Without a
        /// cursor, or with one the server cannot resume from, `on_miss` decides: by
        /// default a snapshot (`snapshot` chunks, then `snapshot_end`) precedes the
        /// live batches. Batches arrive in order with no gaps or repeats.
        ///
        /// Ends with DATA_LOSS when the engine's history is lost and publisher_id
        /// changes, ABORTED when the consumer falls too far behind, and UNAVAILABLE
        /// while the server is starting, recovering or draining. After any of them,
        /// subscribe again with the last cursor.
        pub async fn subscribe_kv_events(
            &mut self,
            request: impl tonic::IntoRequest<super::SubscribeKvEventsRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::KvStreamItem>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/sglang.kv.v1.KvEventService/SubscribeKvEvents",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "sglang.kv.v1.KvEventService",
                "SubscribeKvEvents",
            ));
            self.inner.server_streaming(req, path, codec).await
        }
        /// The retained batches \[from.next_seq, end_seq), then the stream ends.
        /// OUT_OF_RANGE when the window no longer covers `from`, FAILED_PRECONDITION
        /// when `from.publisher_id` is not the current one.
        pub async fn get_kv_events(
            &mut self,
            request: impl tonic::IntoRequest<super::GetKvEventsRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::KvStreamItem>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/sglang.kv.v1.KvEventService/GetKvEvents");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "sglang.kv.v1.KvEventService",
                "GetKvEvents",
            ));
            self.inner.server_streaming(req, path, codec).await
        }
        /// The whole tree at one cut: `start`, `snapshot` chunks, `snapshot_end`.
        pub async fn get_kv_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::GetKvSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::KvStreamItem>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/sglang.kv.v1.KvEventService/GetKvSnapshot");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "sglang.kv.v1.KvEventService",
                "GetKvSnapshot",
            ));
            self.inner.server_streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod kv_event_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with KvEventServiceServer.
    #[async_trait]
    pub trait KvEventService: std::marker::Send + std::marker::Sync + 'static {
        /// The stream's identity, its history window and the tree size.
        async fn get_kv_stream_info(
            &self,
            request: tonic::Request<super::GetKvStreamInfoRequest>,
        ) -> std::result::Result<tonic::Response<super::KvStreamInfo>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
        /// Follow the stream. The first item is `start`. With a cursor inside the
        /// history window the missed batches follow, then live batches. Without a
        /// cursor, or with one the server cannot resume from, `on_miss` decides: by
        /// default a snapshot (`snapshot` chunks, then `snapshot_end`) precedes the
        /// live batches. Batches arrive in order with no gaps or repeats.
        ///
        /// Ends with DATA_LOSS when the engine's history is lost and publisher_id
        /// changes, ABORTED when the consumer falls too far behind, and UNAVAILABLE
        /// while the server is starting, recovering or draining. After any of them,
        /// subscribe again with the last cursor.
        async fn subscribe_kv_events(
            &self,
            request: tonic::Request<super::SubscribeKvEventsRequest>,
        ) -> std::result::Result<tonic::Response<BoxStream<super::KvStreamItem>>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
        /// The retained batches \[from.next_seq, end_seq), then the stream ends.
        /// OUT_OF_RANGE when the window no longer covers `from`, FAILED_PRECONDITION
        /// when `from.publisher_id` is not the current one.
        async fn get_kv_events(
            &self,
            request: tonic::Request<super::GetKvEventsRequest>,
        ) -> std::result::Result<tonic::Response<BoxStream<super::KvStreamItem>>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
        /// The whole tree at one cut: `start`, `snapshot` chunks, `snapshot_end`.
        async fn get_kv_snapshot(
            &self,
            request: tonic::Request<super::GetKvSnapshotRequest>,
        ) -> std::result::Result<tonic::Response<BoxStream<super::KvStreamItem>>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not yet implemented"))
        }
    }
    #[derive(Debug)]
    pub struct KvEventServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> KvEventServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for KvEventServiceServer<T>
    where
        T: KvEventService,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/sglang.kv.v1.KvEventService/GetKvStreamInfo" => {
                    #[allow(non_camel_case_types)]
                    struct GetKvStreamInfoSvc<T: KvEventService>(pub Arc<T>);
                    impl<T: KvEventService>
                        tonic::server::UnaryService<super::GetKvStreamInfoRequest>
                        for GetKvStreamInfoSvc<T>
                    {
                        type Response = super::KvStreamInfo;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetKvStreamInfoRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as KvEventService>::get_kv_stream_info(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = GetKvStreamInfoSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/sglang.kv.v1.KvEventService/SubscribeKvEvents" => {
                    #[allow(non_camel_case_types)]
                    struct SubscribeKvEventsSvc<T: KvEventService>(pub Arc<T>);
                    impl<T: KvEventService>
                        tonic::server::ServerStreamingService<super::SubscribeKvEventsRequest>
                        for SubscribeKvEventsSvc<T>
                    {
                        type Response = super::KvStreamItem;
                        type ResponseStream = BoxStream<super::KvStreamItem>;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SubscribeKvEventsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as KvEventService>::subscribe_kv_events(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SubscribeKvEventsSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/sglang.kv.v1.KvEventService/GetKvEvents" => {
                    #[allow(non_camel_case_types)]
                    struct GetKvEventsSvc<T: KvEventService>(pub Arc<T>);
                    impl<T: KvEventService>
                        tonic::server::ServerStreamingService<super::GetKvEventsRequest>
                        for GetKvEventsSvc<T>
                    {
                        type Response = super::KvStreamItem;
                        type ResponseStream = BoxStream<super::KvStreamItem>;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetKvEventsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as KvEventService>::get_kv_events(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = GetKvEventsSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/sglang.kv.v1.KvEventService/GetKvSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct GetKvSnapshotSvc<T: KvEventService>(pub Arc<T>);
                    impl<T: KvEventService>
                        tonic::server::ServerStreamingService<super::GetKvSnapshotRequest>
                        for GetKvSnapshotSvc<T>
                    {
                        type Response = super::KvStreamItem;
                        type ResponseStream = BoxStream<super::KvStreamItem>;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetKvSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as KvEventService>::get_kv_snapshot(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = GetKvSnapshotSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    let mut response = http::Response::new(tonic::body::Body::default());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }),
            }
        }
    }
    impl<T> Clone for KvEventServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "sglang.kv.v1.KvEventService";
    impl<T> tonic::server::NamedService for KvEventServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
