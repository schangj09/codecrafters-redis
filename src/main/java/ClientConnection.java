import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class ClientConnection {
    Socket clientSocket;
    InputStream inputStream;
    OutputStream outputStream;
    BufferedReader reader;
    BufferedWriter writer;

    public ClientConnection(Socket s) throws IOException {
        clientSocket = s;
        inputStream = s.getInputStream();
        reader = new BufferedReader(new InputStreamReader(inputStream));
        outputStream = s.getOutputStream();
        writer = new BufferedWriter(new OutputStreamWriter(outputStream));
    }
}
