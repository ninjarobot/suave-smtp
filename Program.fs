open System
open Suave
open System.Net
open Suave.Tcp
open Suave.Sockets
open Suave.Sockets.Control
open System.Threading
open System.Linq
open Suave.Logging
open Suave.Logging.Message

let logger = Suave.Logging.Log.create "Suave.Smtp"

/// A record representing a Mail message built up in an SMTP transaction.
type Mail = {
    From : string
    To : string list
    Cc : string list
    Bcc : string list
    Subject : string option
    Body : string option
}

/// A session with an SMTP client.
type Session = {
    Client:string option
    Message:Mail option
}

/// A context for processing an SMTP operation.
type SmtpContext = {
    Session : Session
    Send : string -> SocketOp<unit>
    Read : unit -> Async<Result<string,Error>> 
    Shutdown : Async<unit>
}

/// Between active pattern for parsing SMTP commands
let (|Between|_|) (ends:string*string) (s:string) =
    let start, finish = ends
    if s.StartsWith (start, StringComparison.InvariantCultureIgnoreCase) 
        && s.EndsWith (finish, StringComparison.InvariantCultureIgnoreCase) then
        s.Substring(start.Length, s.Length - (start.Length + finish.Length)) |> Some
    else
        None

/// StartsWith active pattern for parsing headers in DATA contents.
let (|StartsWith|_|) (start:string) (s:string) =
    if s.StartsWith (start, StringComparison.InvariantCultureIgnoreCase) then
        s.Substring(start.Length).TrimStart() |> Some
    else
        None

/// SMTP protocol operations
type OperationType = 
    | Hello of Client:string
    | Continue of string
    | From of string
    | Rcpt of string
    | End
    | Data
    | Closed

let minimalMessage sender = {
    From=sender
    To=List.empty
    Cc=List.empty
    Bcc=List.empty
    Subject=None
    Body=None
}

/// Parsing each supported SMTP operation.
let lineProcessor =
    function
    | "QUIT\r\n" -> End
    | Between ("HELO", "\r\n") client
    | Between ("EHLO", "\r\n") client ->
        logger.info (eventX "Connection from {client}" >> setFieldValue "client" client)
        client |> Hello
    | Between ("MAIL FROM:", "\r\n") sender ->
        logger.info (eventX "Mail message from {sender}" >> setFieldValue "sender" sender)
        sender |> From
    | Between ("RCPT", "\r\n") recipient ->
        logger.info (eventX "Recipient added: {recipient}" >> setFieldValue "recipient" recipient)
        recipient |> Rcpt
    | Between("DATA", "\r\n") _ ->
        Data
    | unknown ->
        logger.error (eventX "Not implemented '{unknown}'" >> setFieldValue "unknown" unknown)
        "504 Not Implemented"
        |> Continue

let private printIfSocketError =
    function
    | Choice2Of2 err -> logger.error (eventX "Socket error: {error}" >> setFieldValue "error" err)
    | _ -> ()

let private getHostName =
    lazy (System.Net.Dns.GetHostName ())

/// Processing each supported SMTP operation with a context for sending and receiving data.
let processSmtpOperation context operation : Async<SmtpContext option>=
    match operation with
    | End ->
        async {
            let! res = "221 Bye" |> context.Send
            res |> printIfSocketError
            return None
        }
    | Closed -> async.Return None
    | Hello client ->
        async {
            let! res =
                getHostName.Value
                |> sprintf "250 %s Ok"
                |> context.Send
            res |> printIfSocketError
            // Start a session.
            return { context with Session={ Client=client |> Some; Message=None } } |> Some
        }
    | From from ->
        async {
            match context.Session.Client with
            | None ->
                let! res = "503 Bad sequence of commands" |> context.Send
                res |> printIfSocketError
                return context |> Some
            | Some _ ->
                let! res = "250 Ok" |> context.Send
                res |> printIfSocketError
                // Start a message in this session.
                return { context with Session={ context.Session with Message=from |> minimalMessage |> Some } } |> Some
        }
    | Rcpt rcpt ->
        async {
            match context.Session.Client, context.Session.Message with
            | Some _, Some message ->
                let! res = "250 Ok" |> context.Send
                res |> printIfSocketError
                match rcpt.TrimStart().Split([|':'|]) with
                | [|rcptType;rcptVal|] ->
                    match rcptType.ToLower() with
                    | "to" ->
                        return { context with Session= { context.Session with Message={ message with To=rcptVal::message.To } |> Some } } |> Some
                    | "cc" ->
                        return { context with Session= { context.Session with Message={ message with Cc=rcptVal::message.Cc } |> Some } } |> Some
                    | "bcc" ->
                        return { context with Session= { context.Session with Message={ message with Bcc=rcptVal::message.Bcc } |> Some } } |> Some
                    | _ -> // Malformed RCPT, just ignore it.
                        return context |> Some
                | _ -> // Malformed RCPT, just ignore it.
                    return context |> Some
            | _ ->
                let! res = "503 Bad sequence of commands" |> context.Send
                res |> printIfSocketError
                return context |> Some
        }
    | Data ->
        async {
            match context.Session.Client, context.Session.Message with
            | Some _, Some message ->
                let! res = "354 Start mail input; end with <CRLF>.<CRLF>" |> context.Send
                res |> printIfSocketError
                /// A bit complicated here, the SMTP DATA operation semantics mean parsing between CRLFs.
                let readEntireBody message = async {
                    let! readBody = context.Read ()
                    match readBody with
                    | Ok entireBody ->
                        let bodyList = entireBody.Split("\r\n", StringSplitOptions.None) |> List.ofArray
                        let rec processBody (bodyList:string list) processingHeaders m =
                            match bodyList with
                            | [] -> m
                            | "."::_ -> m
                            | "" :: tail -> m |> processBody tail false
                            | StartsWith "From:" _ :: tail when processingHeaders -> // ignore - specified in MAIL FROM only
                                m |> processBody tail true
                            | StartsWith "Subject:" subject :: tail when processingHeaders ->
                                { m with Subject=subject |> Some } |> processBody tail true
                            | StartsWith "To:" recipient :: tail when processingHeaders ->
                                { m with To=recipient::m.To } |> processBody tail true
                            | _ :: tail when processingHeaders -> // unknown header...skip
                                m |> processBody tail processingHeaders
                            | body :: tail when not processingHeaders ->
                                match m.Body with
                                | None ->
                                    { m with Body=body |> Some } |> processBody tail false
                                | Some existingBody ->
                                    { m with Body=[existingBody; body] |> String.concat "" |> Some } |> processBody tail false
                        return processBody bodyList true message
                    | Result.Error err ->
                        logger.error (eventX "Error reading body: {error}" >> setFieldValue "error" err)
                        return message
                }
                let! parsedMessage = { message with Body=None; Subject=None } |> readEntireBody
                logger.info (eventX "Parsed message: {msg}" >> setFieldValue "msg" parsedMessage)
                let! res = Guid.NewGuid() |> sprintf "250 OK %O" |> context.Send
                res |> printIfSocketError
                return { context with Session={ context.Session with Message=parsedMessage |> Some } } |> Some
            | _ ->
                let! res = "503 Bad sequence of commands" |> context.Send
                res |> printIfSocketError
                return context |> Some
        }
    | Continue msg -> 
        async {
            let! res = msg |> context.Send
            res |> printIfSocketError
            return context |> Some
        }

/// Start a mail server, passing the function to serve each TCP client.  Binding hardcoded for now.
let startMailServerAsync (config:SuaveConfig) =
    let ip = IPAddress.Parse("0.0.0.0")
    let port = 1025us
    let socketBinding = Suave.Sockets.SocketBinding.create ip port
    let tcpServer =
        config.tcpServerFactory.create(
          config.maxOps, config.bufferSize, config.autoGrow,
          socketBinding)
    /// Handles each client connection
    let serveClient (conn:Suave.Sockets.Connection) =
        async {
            logger.info (eventX "Got socket conn: {binding}" >> setFieldValue "binding" conn)
            /// send function, which appends CRLF and sends as ASCII.
            let send (s:string) =
                s |> sprintf "%s\r\n"
                |> System.Text.Encoding.ASCII.GetBytes
                |> ArraySegment
                |> conn.transport.write
            let buffer = ArraySegment (Array.zeroCreate 256)
            /// buffered read function.
            let read () =
                let sb = System.Text.StringBuilder ()
                let rec readMore () =
                    async {
                        let! b = conn.transport.read buffer
                        match b with
                        | Choice1Of2 bytesRead ->
                            System.Text.Encoding.ASCII.GetString (buffer.Take(bytesRead) |> Array.ofSeq)
                            |> sb.Append |> ignore
                            if bytesRead = buffer.Count then
                                return! readMore ()
                            else
                                return sb.ToString () |> Ok
                        | Choice2Of2 err ->
                            return Result.Error err
                    }
                readMore ()
            /// the context for the TCP session.
            let context = {
                Session = { Client=None; Message=None }
                Send = send
                Read = read
                Shutdown = async { do! conn.transport.shutdown () }
            }
            /// Say hello to the client
            let! headerRes =
                getHostName.Value
                |> sprintf "220 %s Suave SMTP Server"
                |> send
            headerRes |> printIfSocketError
            /// Then start looping
            let rec loop (context:SmtpContext) = async {
                let! msg = read ()
                let operation =
                    match msg with
                    | Ok s ->
                        s |> lineProcessor
                    | Result.Error err ->
                        logger.error (eventX "Error reading socket: {error}" >> setFieldValue "error" err)
                        Closed
                let! result = operation |> processSmtpOperation context
                match result with
                | Some newContext -> do! loop newContext
                | None -> ()
            }
            do! loop context
        }
    startTcpIpServerAsync serveClient socketBinding tcpServer

[<EntryPoint>]
let main argv =
    use wait = new ManualResetEventSlim ()
    use cts = new CancellationTokenSource ()
    let listening, server = startMailServerAsync { defaultConfig with cancellationToken = cts.Token }
    Async.Start (server, cts.Token)
    System.Runtime.Loader.AssemblyLoadContext.Default.add_Unloading (fun _ -> cts.Cancel (); wait.Set ())
    wait.Wait()
    0
