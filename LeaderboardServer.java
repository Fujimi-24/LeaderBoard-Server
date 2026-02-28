import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.sql.*;
import java.util.*;

public class LeaderboardServer {

    private static final String DB_URL = "jdbc:mysql://localhost:3306/asteroids";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "root";

    public static void main(String[] args) throws Exception {

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        server.createContext("/submit", LeaderboardServer::handleSubmit);
        server.createContext("/leaderboard", LeaderboardServer::handleLeaderboard);
        server.createContext("/best", LeaderboardServer::handleBest);
        server.createContext("/rank", LeaderboardServer::handleRank);

        server.setExecutor(null);
        server.start();

        System.out.println("Leaderboard server running on port 8080");
    }

    /* ================= SUBMIT SCORE ================= */

    private static void handleSubmit(HttpExchange exchange) throws IOException {

        try {

            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());

            String username = params.get("username");
            int score = Integer.parseInt(params.get("score"));

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                PreparedStatement getUser =
                        conn.prepareStatement("SELECT id FROM users WHERE username=?");
                getUser.setString(1, username);
                ResultSet rs = getUser.executeQuery();

                int userId;

                if (rs.next()) {
                    userId = rs.getInt("id");
                } else {
                    PreparedStatement insertUser =
                            conn.prepareStatement(
                                    "INSERT INTO users (username, password) VALUES (?, '')",
                                    Statement.RETURN_GENERATED_KEYS);
                    insertUser.setString(1, username);
                    insertUser.executeUpdate();
                    ResultSet keys = insertUser.getGeneratedKeys();
                    keys.next();
                    userId = keys.getInt(1);
                }

                PreparedStatement insertScore =
                        conn.prepareStatement("INSERT INTO high_scores (user_id, score) VALUES (?, ?)");
                insertScore.setInt(1, userId);
                insertScore.setInt(2, score);
                insertScore.executeUpdate();
            }

            send(exchange, "OK");

        } catch (Exception e) {
            e.printStackTrace();
            send(exchange, "ERROR");
        }
    }

    /* ================= LEADERBOARD ================= */

    private static void handleLeaderboard(HttpExchange exchange) throws IOException {

        StringBuilder json = new StringBuilder();
        json.append("[");

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            String sql = """
                    SELECT u.username, MAX(h.score) AS score
                    FROM high_scores h
                    JOIN users u ON h.user_id = u.id
                    GROUP BY u.username
                    ORDER BY score DESC
                    LIMIT 10
                    """;

            ResultSet rs = conn.prepareStatement(sql).executeQuery();

            boolean first = true;

            while (rs.next()) {

                if (!first) json.append(",");
                first = false;

                json.append("{")
                        .append("\"username\":\"").append(rs.getString("username")).append("\",")
                        .append("\"score\":").append(rs.getInt("score"))
                        .append("}");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        json.append("]");
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        send(exchange, json.toString());
    }

    /* ================= BEST SCORE ================= */

    private static void handleBest(HttpExchange exchange) throws IOException {

        int best = 0;

        try {

            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String username = params.get("username");

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                String sql = """
                        SELECT MAX(h.score) AS best
                        FROM high_scores h
                        JOIN users u ON h.user_id = u.id
                        WHERE u.username = ?
                        """;

                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1, username);
                ResultSet rs = stmt.executeQuery();

                if (rs.next())
                    best = rs.getInt("best");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        send(exchange, String.valueOf(best));
    }

    /* ================= RANK ================= */

    private static void handleRank(HttpExchange exchange) throws IOException {

        int rank = 0;

        try {

            Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
            String username = params.get("username");

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                String sql = """
                        SELECT COUNT(*) + 1 AS rank_position
                        FROM (
                            SELECT u.username, MAX(h.score) AS score
                            FROM high_scores h
                            JOIN users u ON h.user_id = u.id
                            GROUP BY u.username
                        ) ranked
                        WHERE ranked.score > (
                            SELECT MAX(h2.score)
                            FROM high_scores h2
                            JOIN users u2 ON h2.user_id = u2.id
                            WHERE u2.username = ?
                        )
                        """;

                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1, username);
                ResultSet rs = stmt.executeQuery();

                if (rs.next())
                    rank = rs.getInt("rank_position");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        send(exchange, String.valueOf(rank));
    }

    /* ================= UTIL ================= */

    private static Map<String, String> parseQuery(String query) throws UnsupportedEncodingException {

        Map<String, String> map = new HashMap<>();
        if (query == null) return map;

        for (String param : query.split("&")) {
            String[] pair = param.split("=");
            if (pair.length == 2) {
                map.put(
                        URLDecoder.decode(pair[0], "UTF-8"),
                        URLDecoder.decode(pair[1], "UTF-8")
                );
            }
        }

        return map;
    }

    private static void send(HttpExchange exchange, String response) throws IOException {

        exchange.sendResponseHeaders(200, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }
}