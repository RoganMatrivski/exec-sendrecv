use eyre::Context;

const IROH_BROKER_URL: &str = "https://iroh-broker.rgmtrv.my.id";

pub async fn set(ticket: iroh_tickets::endpoint::EndpointTicket) -> eyre::Result<String> {
    let endpoint_id = ticket.endpoint_addr().id.to_string();
    reqwest::Client::default()
        .post(format!("{IROH_BROKER_URL}/register"))
        .body(ticket.to_string() + "," + &endpoint_id)
        .send()
        .await
        .wrap_err("Failed to send ticket to broker")?
        .text()
        .await
        .wrap_err("Failed to get code from broker")
}

pub async fn get(
    code: impl std::fmt::Display,
) -> eyre::Result<iroh_tickets::endpoint::EndpointTicket> {
    let code = code.to_string().split_whitespace().collect::<String>();
    reqwest::Client::default()
        .post(format!("{IROH_BROKER_URL}/fetch/{code}"))
        .send()
        .await
        .wrap_err("Failed to send ticket to broker")?
        .text()
        .await
        .wrap_err("Failed to get response text from broker")?
        .parse::<iroh_tickets::endpoint::EndpointTicket>()
        .wrap_err("Failed to parse ticket from broker response")
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_clean_code_whitespace() {
        let raw_code = "  abc  123 \t def\n ";
        let cleaned = raw_code.split_whitespace().collect::<String>();
        assert_eq!(cleaned, "abc123def");
    }
}


