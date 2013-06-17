package com.dedupeer.gui.component;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;

import javax.swing.JLabel;

import com.dedupeer.utils.Range;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class Legend extends JLabel {
	
	private static final long serialVersionUID = -8840944606397896999L;

	public final static int TYPE_CHUNK = 0;
	public final static int TYPE_REFERENCE = 1;
	
	private static final String space = "       ";
	private static final String chunkText = space + "Chunks with content";
	private static final String referenceText = space + "References to other chunks";
	
	private static final int boxWidth = 12, boxHeight = 12; 
	private Color boxColor;
	
	private String text;
	
	/**
	 * Creates a legend component to describe the modification bar.
	 * @param type Type of legend
	 */
	public Legend(int type) {
		super((type == TYPE_CHUNK ? chunkText : type == TYPE_REFERENCE ? referenceText : ""));
		text = (type == TYPE_CHUNK ? chunkText : type == TYPE_REFERENCE ? referenceText : "");
		boxColor = type == TYPE_CHUNK ? new Color(0x078ace) : new Color(0xd8d8d8);
		
	}
	
	@Override
	public void paintComponent(Graphics g) {
		 Graphics2D g2 = (Graphics2D) g;
		 
		 g2.drawString(text, 0, (this.getHeight()>>1) + 3);
		 
		 g2.setColor(boxColor);
		 g2.fillRect(boxWidth>>1, 0, boxWidth, boxHeight);
		 		 		 
		 g2.setColor(Color.black);
		 g2.drawRect(boxWidth>>1, 0, boxWidth -1, boxHeight - 1);		 
	}
}
